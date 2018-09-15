package coop.rchain.rholang.interpreter

import java.nio.file.Files

import com.google.protobuf.ByteString
import coop.rchain.catscontrib.TaskContrib._
import coop.rchain.crypto.codec.Base16
import coop.rchain.crypto.hash.Blake2b512Random
import coop.rchain.models.Channel.ChannelInstance._
import coop.rchain.models.Connective.ConnectiveInstance._
import coop.rchain.models.Expr.ExprInstance._
import coop.rchain.models.TaggedContinuation.TaggedCont.ParBody
import coop.rchain.models.Var.VarInstance._
import coop.rchain.models._
import coop.rchain.models.rholang.implicits._
import coop.rchain.rholang.interpreter.Reducer.DebruijnInterpreter
import coop.rchain.rholang.interpreter.Runtime.{RhoContext, RhoISpace}
import coop.rchain.rholang.interpreter.accounting.{CostAccount, CostAccountingAlg}
import coop.rchain.rholang.interpreter.errors._
import coop.rchain.rholang.interpreter.storage.TuplespaceAlg
import coop.rchain.rholang.interpreter.storage.implicits._
import coop.rchain.rspace._
import coop.rchain.rspace.history.Branch
import coop.rchain.rspace.internal.{Datum, Row, WaitingContinuation}
import coop.rchain.rspace.pure.PureRSpace
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.{Assertion, FlatSpec, Matchers}

import scala.collection.immutable.BitSet
import scala.collection.mutable.HashMap
import scala.concurrent.Await
import scala.concurrent.duration._

trait PersistentStoreTester {
  def withTestSpace[R](f: RhoISpace => R): R = {
    val dbDir               = Files.createTempDirectory("rholang-interpreter-test-")
    val context: RhoContext = Context.create(dbDir, mapSize = 1024L * 1024L * 10)
    val space = RSpace.create[Channel,
                              BindPattern,
                              OutOfPhlogistonsError.type,
                              ListChannelWithRandom,
                              ListChannelWithRandom,
                              TaggedContinuation](context, Branch("test"))
    try {
      f(space)
    } finally {
      space.close()
      context.close()
    }
  }
}

class ReducerSpec extends FlatSpec with Matchers with PersistentStoreTester {
  implicit val rand: Blake2b512Random = Blake2b512Random(Array.empty[Byte])

  def withReducer(test: FixtureState => Assertion): Unit =
    withTestSpace { space =>
      val pureSpace: Runtime.RhoPureSpace = new PureRSpace(space)
      implicit val costAccounting: CostAccountingAlg[Task] =
        CostAccountingAlg.unsafe[Task](CostAccount(Integer.MAX_VALUE))
      implicit val errorLog  = new ErrorLog()
      lazy val tuplespaceAlg = TuplespaceAlg.rspaceTuplespace[Task, Task.Par](pureSpace, dispatcher)
      lazy val reducer: ChargingReducer[Task] =
        new Reducer.DebruijnInterpreter[Task, Task.Par](tuplespaceAlg,
                                                        costAccounting,
                                                        Registry.testingUrnMap)
      lazy val dispatcher: Dispatch[Task, ListChannelWithRandom, TaggedContinuation] =
        RholangOnlyDispatcher.create[Task, Task.Par](reducer)
      lazy val fixtureState = FixtureState(reducer, errorLog, tuplespaceAlg, space, costAccounting)
      test(fixtureState)
    }

  private case class FixtureState(reducer: ChargingReducer[Task],
                                  errorLog: ErrorLog,
                                  tuplespace: TuplespaceAlg[Task],
                                  space: RhoISpace,
                                  costAccounting: CostAccountingAlg[Task])

  "evalExpr" should "handle simple addition" in withReducer { fixtureState =>
    import fixtureState._
    val addExpr      = EPlus(GInt(7), GInt(8))
    implicit val env = Env[Par]()
    val result       = reducer.evalExpr(addExpr).unsafeRunSync
    val expected     = Seq(Expr(GInt(15)))
    result.exprs should be(expected)
    errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "evalExpr" should "handle long addition" in withReducer { fixtureState =>
    import fixtureState._
    val addExpr      = EPlus(GInt(Int.MaxValue), GInt(Int.MaxValue))
    implicit val env = Env[Par]()
    val result       = reducer.evalExpr(addExpr).unsafeRunSync
    val expected     = Seq(Expr(GInt(2 * Int.MaxValue.toLong)))
    result.exprs should be(expected)
    errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "evalExpr" should "leave ground values alone" in withReducer { fixtureState =>
    import fixtureState._
    val groundExpr   = GInt(7)
    implicit val env = Env[Par]()
    val result       = reducer.evalExpr(groundExpr).unsafeRunSync
    val expected     = Seq(Expr(GInt(7)))
    result.exprs should be(expected)
    errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "evalExpr" should "handle equality between arbitrary processes" in withReducer { fixtureState =>
    import fixtureState._
    val eqExpr       = EEq(GPrivateBuilder("private_name"), GPrivateBuilder("private_name"))
    implicit val env = Env[Par]()
    val result       = reducer.evalExpr(eqExpr).unsafeRunSync
    val expected     = Seq(Expr(GBool(true)))
    result.exprs should be(expected)
    errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "evalExpr" should "substitute before comparison." in withReducer { fixtureState =>
    import fixtureState._
    val eqExpr            = EEq(EVar(BoundVar(0)), EVar(BoundVar(1)))
    implicit val emptyEnv = Env.makeEnv(Par(), Par())
    val result            = reducer.evalExpr(eqExpr).unsafeRunSync
    val expected          = Seq(Expr(GBool(true)))
    result.exprs should be(expected)
    errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "eval of Bundle" should "evaluate contents of bundle" in withReducer { fixtureState =>
    import fixtureState._
    val splitRand = rand.splitByte(0)

    val bundleSend =
      Bundle(Send(Quote(GString("channel")), List(GInt(7), GInt(8), GInt(9)), false, BitSet()))
    val interpreter  = reducer
    implicit val env = Env[Par]()
    val resultTask   = interpreter.eval(bundleSend)(env, splitRand)
    val inspectTask = for {
      _ <- resultTask
    } yield space.store.toMap
    val result = inspectTask.unsafeRunSync

    val channel = Channel(Quote(GString("channel")))

    result should be(
      HashMap(
        List(channel) ->
          Row(
            List(Datum.create(
              channel,
              ListChannelWithRandom(Seq(Quote(GInt(7)), Quote(GInt(8)), Quote(GInt(9))), splitRand),
              false)),
            List()
          )
      ))
    errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  it should "throw an error if names are used against their polarity" in withReducer {
    fixtureState =>
      import fixtureState._
      /* for (n <- @bundle+ { y } ) { }  -> for (n <- y) { }
       */
      val y = GString("y")
      val receive = Receive(
        Seq(ReceiveBind(Seq(Quote(Par())), Quote(Bundle(y, readFlag = false, writeFlag = true)))),
        Par())

      implicit val env  = Env[Par]()
      val receiveResult = reducer.eval(receive).map(_ => space.store.toMap).unsafeRunSync

      receiveResult should be(HashMap.empty)
      errorLog.readAndClearErrorVector should be(
        Vector(ReduceError("Trying to read from non-readable channel.")))

      /* @bundle- { x } !(7) -> x!(7)
       */
      val x = GString("channel")
      val send =
        Send(Channel(Quote(Bundle(x, writeFlag = false, readFlag = true))), Seq(Expr(GInt(7))))

      val sendResult = reducer.eval(send).map(_ => space.store.toMap).unsafeRunSync
      sendResult should be(HashMap.empty)
      errorLog.readAndClearErrorVector should be(
        Vector(ReduceError("Trying to send on non-writeable channel.")))
  }

  "eval of Send" should "place something in the tuplespace." in withReducer { fixtureState =>
    import fixtureState._
    val splitRand = rand.splitByte(0)

    val send =
      Send(Quote(GString("channel")), List(GInt(7), GInt(8), GInt(9)), false, BitSet())

    implicit val env = Env[Par]()
    val resultTask   = reducer.eval(send)(env, splitRand)
    val inspectTask = for {
      _ <- resultTask
    } yield space.store.toMap
    val result = inspectTask.unsafeRunSync

    val channel = Channel(Quote(GString("channel")))

    result should be(
      HashMap(
        List(channel) ->
          Row(
            List(Datum.create(
              channel,
              ListChannelWithRandom(Seq(Quote(GInt(7)), Quote(GInt(8)), Quote(GInt(9))), splitRand),
              false)),
            List()
          )
      ))
    errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  it should "verify that Bundle is writeable before sending on Bundle " in withReducer {
    fixtureState =>
      import fixtureState._
      val splitRand = rand.splitByte(0)
      /* @bundle+ { x } !(7) -> x!(7)
       */
      val x = GString("channel")
      val send =
        Send(Channel(Quote(Bundle(x, writeFlag = true, readFlag = false))), Seq(Expr(GInt(7))))

      implicit val env = Env[Par]()
      val result       = reducer.eval(send)(env, splitRand).map(_ => space.store.toMap).unsafeRunSync

      val channel = Channel(Quote(x))

      result should be(
        HashMap(List(channel) -> Row(
          List(Datum.create(channel, ListChannelWithRandom(Seq(Quote(GInt(7))), splitRand), false)),
          List())))
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "eval of single channel Receive" should "place something in the tuplespace." in withReducer {
    fixtureState =>
      import fixtureState._
      val splitRand = rand.splitByte(0)

      val receive =
        Receive(Seq(
                  ReceiveBind(Seq(ChanVar(FreeVar(0)), ChanVar(FreeVar(1)), ChanVar(FreeVar(2))),
                              Quote(GString("channel")))),
                Par(),
                false,
                3,
                BitSet())

      implicit val env = Env[Par]()
      val resultTask   = reducer.eval(receive)(env, splitRand)
      val inspectTask = for {
        _ <- resultTask
      } yield space.store.toMap
      val result = inspectTask.unsafeRunSync

      val channels = List(Channel(Quote(GString("channel"))))

      result should be(
        HashMap(
          channels ->
            Row(
              List(),
              List(
                WaitingContinuation
                  .create[Channel, BindPattern, TaggedContinuation](
                    channels,
                    List(
                      BindPattern(List(Channel(ChanVar(FreeVar(0))),
                                       Channel(ChanVar(FreeVar(1))),
                                       Channel(ChanVar(FreeVar(2)))),
                                  None)),
                    TaggedContinuation(ParBody(ParWithRandom(Par(), splitRand))),
                    false
                  )
              )
            )
        ))
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  it should "verify that bundle is readable if receiving on Bundle" in withReducer { fixtureState =>
    import fixtureState._

    val splitRand = rand.splitByte(1)
    /* for (@Nil <- @bundle- { y } ) { }  -> for (n <- y) { }
     */

    val y = GString("y")
    val receive = Receive(binds = Seq(
                            ReceiveBind(
                              patterns = Seq(Quote(Par())),
                              source = Quote(Bundle(y, readFlag = true, writeFlag = false))
                            )),
                          body = Par())

    implicit val env = Env[Par]()
    val result       = reducer.eval(receive)(env, splitRand).map(_ => space.store.toMap).unsafeRunSync

    val channels = List(Channel(Quote(y)))

    result should be(
      HashMap(
        channels ->
          Row(
            List(),
            List(
              WaitingContinuation.create[Channel, BindPattern, TaggedContinuation](
                channels,
                List(BindPattern(List(Channel(Quote(Par()))), None)),
                TaggedContinuation(ParBody(ParWithRandom(Par(), splitRand))),
                false))
          )
      ))
    errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "eval of Send | Receive" should "meet in the tuplespace and proceed." in withReducer {
    fixtureState =>
      import fixtureState._

      val splitRand0 = rand.splitByte(0)
      val splitRand1 = rand.splitByte(1)
      val mergeRand  = Blake2b512Random.merge(Seq(splitRand1, splitRand0))
      val send =
        Send(Quote(GString("channel")), List(GInt(7), GInt(8), GInt(9)), false, BitSet())
      val receive = Receive(
        Seq(
          ReceiveBind(Seq(ChanVar(FreeVar(0)), ChanVar(FreeVar(1)), ChanVar(FreeVar(2))),
                      Quote(GString("channel")),
                      freeCount = 3)),
        Send(Quote(GString("result")), List(GString("Success")), false, BitSet()),
        false,
        3,
        BitSet()
      )

      implicit val env = Env[Par]()
      val inspectTaskSendFirst = for {
        _ <- reducer.eval(send)(env, splitRand0)
        _ <- reducer.eval(receive)(env, splitRand1)
      } yield space.store.toMap
      val sendFirstResult = inspectTaskSendFirst.unsafeRunSync

      val channel = Channel(Quote(GString("result")))

      sendFirstResult should be(
        HashMap(
          List(channel) ->
            Row(List(
                  Datum.create(channel,
                               ListChannelWithRandom(Seq(Quote(GString("Success"))), mergeRand),
                               false)),
                List())
        )
      )

      space.clear()
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])

      val emptyEnv = Env[Par]()
      val inspectTaskReceiveFirst = for {
        _ <- reducer.eval(receive)(emptyEnv, splitRand1)
        _ <- reducer.eval(send)(emptyEnv, splitRand0)
      } yield space.store.toMap
      val receiveFirstResult = inspectTaskReceiveFirst.unsafeRunSync

      receiveFirstResult should be(
        HashMap(
          List(channel) ->
            Row(List(
                  Datum.create(channel,
                               ListChannelWithRandom(Seq(Quote(GString("Success"))), mergeRand),
                               false)),
                List())
        )
      )
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "eval of Send | Receive" should "when whole list is bound to list remainder, meet in the tuplespace and proceed. (RHOL-422)" in withReducer {
    fixtureState =>
      import fixtureState._
      // for(@[...a] <- @"channel") { … } | @"channel"!([7,8,9])

      val splitRand0 = rand.splitByte(0)
      val splitRand1 = rand.splitByte(1)
      val mergeRand  = Blake2b512Random.merge(Seq(splitRand1, splitRand0))
      // format: off
      val send =
        Send(Quote(GString("channel")), List(Par(exprs = Seq(Expr(EListBody(EList(Seq(GInt(7), GInt(8), GInt(9)))))))), false, BitSet())
      val receive = Receive(
        Seq(
          ReceiveBind(Seq(Quote(Par(exprs = Seq(EListBody(EList(connectiveUsed = true, remainder = Some(FreeVar(0)))))))),
            Quote(GString("channel")),
            freeCount = 1)),
          Send(Quote(GString("result")), List(GString("Success")), false, BitSet()),
          false,
          1,
          BitSet()
      )
      // format: on

      implicit val env = Env[Par]()
      val inspectTaskSendFirst = for {
        _ <- reducer.eval(send)(env, splitRand0)
        _ <- reducer.eval(receive)(env, splitRand1)
      } yield space.store.toMap
      val sendFirstResult = inspectTaskSendFirst.unsafeRunSync

      val channel = Channel(Quote(GString("result")))

      sendFirstResult should be(
        HashMap(
          List(channel) ->
            Row(List(
                  Datum.create(channel,
                               ListChannelWithRandom(Seq(Quote(GString("Success"))), mergeRand),
                               false)),
                List())
        )
      )

      space.clear()
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])

      // implicit val env = Env[Par]()
      val inspectTaskReceiveFirst = for {
        _ <- reducer.eval(receive)(env, splitRand1)
        _ <- reducer.eval(send)(env, splitRand0)
      } yield space.store.toMap
      val receiveFirstResult = inspectTaskReceiveFirst.unsafeRunSync

      receiveFirstResult should be(
        HashMap(
          List(channel) ->
            Row(List(
                  Datum.create(channel,
                               ListChannelWithRandom(Seq(Quote(GString("Success"))), mergeRand),
                               false)),
                List())
        )
      )
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "eval of Send on (7 + 8) | Receive on 15" should "meet in the tuplespace and proceed." in withReducer {
    fixtureState =>
      import fixtureState._

      val splitRand0 = rand.splitByte(0)
      val splitRand1 = rand.splitByte(1)
      val mergeRand  = Blake2b512Random.merge(Seq(splitRand1, splitRand0))
      val send =
        Send(Quote(EPlus(GInt(7), GInt(8))), List(GInt(7), GInt(8), GInt(9)), false, BitSet())
      val receive = Receive(
        Seq(
          ReceiveBind(Seq(ChanVar(FreeVar(0)), ChanVar(FreeVar(1)), ChanVar(FreeVar(2))),
                      Quote(GInt(15)),
                      freeCount = 3)),
        Send(Quote(GString("result")), List(GString("Success")), false, BitSet()),
        false,
        3,
        BitSet()
      )

      implicit val env = Env[Par]()
      val inspectTaskSendFirst = for {
        _ <- reducer.eval(send)(env, splitRand0)
        _ <- reducer.eval(receive)(env, splitRand1)
      } yield space.store.toMap
      val sendFirstResult = inspectTaskSendFirst.unsafeRunSync

      val channel = Channel(Quote(GString("result")))

      sendFirstResult should be(
        HashMap(
          List(channel) ->
            Row(List(
                  Datum.create(channel,
                               ListChannelWithRandom(Seq(Quote(GString("Success"))), mergeRand),
                               false)),
                List())
        )
      )

      space.clear()
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])

      // implicit val env = Env[Par]()
      val inspectTaskReceiveFirst = for {
        _ <- reducer.eval(receive)(env, splitRand1)
        _ <- reducer.eval(send)(env, splitRand0)
      } yield space.store.toMap
      val receiveFirstResult = inspectTaskReceiveFirst.unsafeRunSync

      receiveFirstResult should be(
        HashMap(
          List(channel) ->
            Row(List(
                  Datum.create(channel,
                               ListChannelWithRandom(Seq(Quote(GString("Success"))), mergeRand),
                               false)),
                List())
        )
      )
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "eval of Send of Receive | Receive" should "meet in the tuplespace and proceed." in withReducer {
    fixtureState =>
      import fixtureState._

      val baseRand   = rand.splitByte(2)
      val splitRand0 = baseRand.splitByte(0)
      val splitRand1 = baseRand.splitByte(1)
      val mergeRand  = Blake2b512Random.merge(Seq(splitRand1, splitRand0))
      val simpleReceive = Receive(
        Seq(ReceiveBind(Seq(Quote(GInt(2))), Quote(GInt(2)))),
        Par(),
        false,
        0,
        BitSet()
      )
      val send =
        Send(Quote(GInt(1)), Seq[Par](simpleReceive), false, BitSet())
      val receive = Receive(
        Seq(ReceiveBind(Seq(ChanVar(FreeVar(0))), Quote(GInt(1)), freeCount = 1)),
        EEvalBody(ChanVar(BoundVar(0))),
        false,
        1,
        BitSet()
      )

      val interpreter  = reducer
      implicit val env = Env[Par]()
      val inspectTaskSendFirst = for {
        _ <- interpreter.eval(send)(env, splitRand0)
        _ <- interpreter.eval(receive)(env, splitRand1)
      } yield space.store.toMap
      val sendFirstResult = inspectTaskSendFirst.unsafeRunSync

      val channels = List(Channel(Quote(GInt(2))))

      // Because they are evaluated separately, nothing is split.
      sendFirstResult should be(
        HashMap(
          channels ->
            Row(
              List(),
              List(
                WaitingContinuation.create[Channel, BindPattern, TaggedContinuation](
                  channels,
                  List(BindPattern(List(Quote(GInt(2))))),
                  TaggedContinuation(ParBody(ParWithRandom(Par(), mergeRand))),
                  false)
              )
            )
        )
      )

      space.clear()
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])

      // implicit val env = Env[Par]()
      val inspectTaskReceiveFirst = for {
        _ <- reducer.eval(receive)(env, splitRand1)
        _ <- reducer.eval(send)(env, splitRand0)
      } yield space.store.toMap
      val receiveFirstResult = inspectTaskReceiveFirst.unsafeRunSync

      receiveFirstResult should be(
        HashMap(
          channels ->
            Row(
              List(),
              List(
                WaitingContinuation.create[Channel, BindPattern, TaggedContinuation](
                  channels,
                  List(BindPattern(List(Quote(GInt(2))))),
                  TaggedContinuation(ParBody(ParWithRandom(Par(), mergeRand))),
                  false))
            )
        )
      )

      space.clear()
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])

      val inspectTaskReceiveSecond = for {
        _ <- reducer.eval(Par(receives = Seq(receive), sends = Seq(send)))(Env[Par](), baseRand)
      } yield space.store.toMap
      val bothResult = inspectTaskReceiveSecond.unsafeRunSync

      bothResult should be(
        HashMap(
          channels ->
            Row(
              List(),
              List(
                WaitingContinuation.create[Channel, BindPattern, TaggedContinuation](
                  channels,
                  List(BindPattern(List(Quote(GInt(2))))),
                  TaggedContinuation(ParBody(ParWithRandom(Par(), mergeRand))),
                  false))
            )
        )
      )
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "Simple match" should "capture and add to the environment." in withReducer { fixtureState =>
    import fixtureState._

    val splitRand = rand.splitByte(0)

    val pattern = Send(ChanVar(FreeVar(0)), List(GInt(7), EVar(FreeVar(1))), false, BitSet())
      .withConnectiveUsed(true)
    val sendTarget =
      Send(ChanVar(BoundVar(1)), List(GInt(7), EVar(BoundVar(0))), false, BitSet(0, 1))
    val matchTerm = Match(
      sendTarget,
      List(
        MatchCase(
          pattern,
          Send(Quote(GString("result")),
               List(EEvalBody(ChanVar(BoundVar(1))), EVar(BoundVar(0))),
               false,
               BitSet(0, 1)),
          freeCount = 2
        )),
      BitSet()
    )
    implicit val env = Env.makeEnv[Par](GPrivateBuilder("one"), GPrivateBuilder("zero"))

    val matchTask = reducer.eval(matchTerm)(env, splitRand)
    val inspectTask = for {
      _ <- matchTask
    } yield space.store.toMap
    val result = inspectTask.unsafeRunSync

    val channel = Channel(Quote(GString("result")))

    result should be(
      HashMap(
        List(channel) ->
          Row(
            List(
              Datum.create(channel,
                           ListChannelWithRandom(Seq(Quote(GPrivateBuilder("one")),
                                                     Quote(GPrivateBuilder("zero"))),
                                                 splitRand),
                           false)),
            List()
          )
      )
    )
    errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "eval of Send | Send | Receive join" should "meet in the tuplespace and proceed." in withReducer {
    fixtureState =>
      import fixtureState._
      val splitRand0 = rand.splitByte(0)
      val splitRand1 = rand.splitByte(1)
      val splitRand2 = rand.splitByte(2)
      val mergeRand  = Blake2b512Random.merge(Seq(splitRand2, splitRand0, splitRand1))
      val send1 =
        Send(Quote(GString("channel1")), List(GInt(7), GInt(8), GInt(9)), false, BitSet())
      val send2 =
        Send(Quote(GString("channel2")), List(GInt(7), GInt(8), GInt(9)), false, BitSet())
      val receive = Receive(
        Seq(
          ReceiveBind(Seq(ChanVar(FreeVar(0)), ChanVar(FreeVar(1)), ChanVar(FreeVar(2))),
                      Quote(GString("channel1")),
                      freeCount = 3),
          ReceiveBind(Seq(ChanVar(FreeVar(0)), ChanVar(FreeVar(1)), ChanVar(FreeVar(2))),
                      Quote(GString("channel2")),
                      freeCount = 3)
        ),
        Send(Quote(GString("result")), List(GString("Success")), false, BitSet()),
        false,
        3,
        BitSet()
      )

      val envF = Env[Par]()
      val inspectTaskSendFirst = for {
        _ <- reducer.eval(send1)(envF, splitRand0)
        _ <- reducer.eval(send2)(envF, splitRand1)
        _ <- reducer.eval(receive)(envF, splitRand2)
      } yield space.store.toMap
      val sendFirstResult = inspectTaskSendFirst.unsafeRunSync

      val channel = Channel(Quote(GString("result")))

      sendFirstResult should be(
        HashMap(
          List(channel) ->
            Row(List(
                  Datum.create(channel,
                               ListChannelWithRandom(Seq(Quote(GString("Success"))), mergeRand),
                               false)),
                List())
        )
      )

      space.clear()
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])

      val envS = Env[Par]()
      val inspectTaskReceiveFirst = for {
        _ <- reducer.eval(receive)(envS, splitRand2)
        _ <- reducer.eval(send1)(envS, splitRand0)
        _ <- reducer.eval(send2)(envS, splitRand1)
      } yield space.store.toMap
      val receiveFirstResult = inspectTaskReceiveFirst.unsafeRunSync

      receiveFirstResult should be(
        HashMap(
          List(channel) ->
            Row(List(
                  Datum.create(channel,
                               ListChannelWithRandom(Seq(Quote(GString("Success"))), mergeRand),
                               false)),
                List())
        )
      )

      space.clear()
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])

      val envT = Env[Par]()
      val inspectTaskInterleaved = for {
        _ <- reducer.eval(send1)(envT, splitRand0)
        _ <- reducer.eval(receive)(envT, splitRand2)
        _ <- reducer.eval(send2)(envT, splitRand1)
      } yield space.store.toMap
      val interleavedResult = inspectTaskInterleaved.unsafeRunSync

      interleavedResult should be(
        HashMap(
          List(channel) ->
            Row(List(
                  Datum.create(channel,
                               ListChannelWithRandom(Seq(Quote(GString("Success"))), mergeRand),
                               false)),
                List())
        )
      )
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "eval of Send with remainder receive" should "capture the remainder." in withReducer {
    fixtureState =>
      import fixtureState._

      val splitRand0 = rand.splitByte(0)
      val splitRand1 = rand.splitByte(1)
      val mergeRand  = Blake2b512Random.merge(Seq(splitRand1, splitRand0))
      val send =
        Send(Quote(GString("channel")), List(GInt(7), GInt(8), GInt(9)), false, BitSet())
      val receive =
        Receive(Seq(ReceiveBind(Seq(), Quote(GString("channel")), Some(FreeVar(0)), freeCount = 1)),
                Send(Quote(GString("result")), Seq(EVar(BoundVar(0)))))

      implicit val env = Env[Par]()
      val task = for {
        _ <- reducer.eval(receive)(env, splitRand1)
        _ <- reducer.eval(send)(env, splitRand0)
      } yield space.store.toMap
      val result = task.unsafeRunSync

      val channel = Channel(Quote(GString("result")))

      // format: off
   result should be(
     HashMap(
       List(channel) ->
         Row(List(
           Datum.create(channel,
             ListChannelWithRandom(Seq(Quote(EList(List(GInt(7), GInt(8), GInt(9))))), mergeRand),
             false)),
           List())
     )
   )
   // format: on
      errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "eval of nth method" should "pick out the nth item from a list" in withReducer { fixtureState =>
    import fixtureState._

    val splitRand = rand.splitByte(0)
    val nthCall: Expr =
      EMethod("nth", EList(List(GInt(7), GInt(8), GInt(9), GInt(10))), List[Par](GInt(2)))

    implicit val env = Env[Par]()

    val directResult: Par = reducer.evalExprToPar(nthCall).unsafeRunSync

    val expectedResult: Par = GInt(9)
    directResult should be(expectedResult)

    val nthCallEvalToSend: Expr =
      EMethod("nth",
              EList(
                List(GInt(7),
                     Send(Quote(GString("result")), List(GString("Success")), false, BitSet()),
                     GInt(9),
                     GInt(10))),
              List[Par](GInt(1)))

    // implicit val env = Env[Par]()
    val nthTask = reducer.eval(nthCallEvalToSend)(env, splitRand)
    val inspectTask = for {
      _ <- nthTask
    } yield space.store.toMap
    val indirectResult = inspectTask.unsafeRunSync

    val channel = Channel(Quote(GString("result")))

    indirectResult should be(
      HashMap(
        List(channel) ->
          Row(List(
                Datum.create(channel,
                             ListChannelWithRandom(Seq(Quote(GString("Success"))), splitRand),
                             false)),
              List())
      )
    )
    errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
  }

  "eval of New" should "use deterministic names and provide urn-based resources" in withReducer {
    fixtureState =>
      import fixtureState._

      val splitRand   = rand.splitByte(42)
      val resultRand  = rand.splitByte(42)
      val chosenName  = resultRand.next
      val result0Rand = resultRand.splitByte(0)
      val result1Rand = resultRand.splitByte(1)
      val newProc: New =
        New(
          bindCount = 2,
          uri = List("rho:test:foo"),
          p = Par(
            sends = List(
              Send(Quote(GString("result0")), List(EVar(BoundVar(0))), locallyFree = BitSet(0)),
              Send(Quote(GString("result1")), List(EVar(BoundVar(1))), locallyFree = BitSet(1))
            ),
            locallyFree = BitSet(0, 1)
          )
        )

      def byteName(b: Byte): Par = GPrivate(ByteString.copyFrom(Array[Byte](b)))

      implicit val env         = Env[Par]()
      implicit val functorTell = errorLog
      val urnReducer =
        new DebruijnInterpreter[Task, Task.Par](tuplespace,
                                                costAccounting,
                                                Map("rho:test:foo" -> byteName(42)))
      val nthTask = urnReducer.eval(newProc)(env, splitRand)
      val inspectTask = for {
        _ <- nthTask
      } yield space.store.toMap
      val result = inspectTask.unsafeRunSync

      val channel0 = Channel(Quote(GString("result0")))
      val channel1 = Channel(Quote(GString("result1")))
      // format: off
   result should be(
     HashMap(
       List(channel0) ->
         Row(
           List(
             Datum.create(
               channel0,
               ListChannelWithRandom(Seq(Quote(GPrivate(ByteString.copyFrom(Array[Byte](42))))), result0Rand),
               false)),
           List()),
       List(channel1) ->
         Row(
           List(
             Datum.create(
               channel1,
               ListChannelWithRandom(Seq(Quote(GPrivate(ByteString.copyFrom(chosenName)))), result1Rand),
               false)),
           List())
     )
   )
   // format: on
  }

  // format: off
  "eval of nth method in send position" should "change what is sent" in withReducer { fixtureState =>
  // format: on
    import fixtureState._

    val splitRand = rand.splitByte(0)
    val nthCallEvalToSend: Expr =
      EMethod("nth",
              EList(
                List(GInt(7),
                     Send(Quote(GString("result")), List(GString("Success")), false, BitSet()),
                     GInt(9),
                     GInt(10))),
              List[Par](GInt(1)))
    val send: Par =
      Send(Quote(GString("result")), List[Par](nthCallEvalToSend), false, BitSet())

    implicit val env = Env[Par]()
    val nthTask      = reducer.eval(send)(env, splitRand)
    val inspectTask = for {
      _ <- nthTask
    } yield space.store.toMap
    val result = inspectTask.unsafeRunSync

    val channel = Channel(Quote(GString("result")))

    // format: off
     result should be(
       HashMap(
         List(channel) ->
           Row(
             List(
               Datum.create(
                 channel,
                 ListChannelWithRandom(Seq(
                   Quote(Send(Quote(GString("result")), List(GString("Success")), false, BitSet()))), splitRand),
                 false)),
             List())
       )
     )
    // format: off
    errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "eval of a method" should "substitute target before evaluating" in withReducer { fixtureState =>
   import fixtureState._
   val splitRand = rand.splitByte(0)
   val hexToBytesCall: Expr =
     EMethod("hexToBytes", Expr(EVarBody(EVar(Var(BoundVar(0))))))
   
     implicit val env = Env.makeEnv[Par](Expr(GString("deadbeef")))
     val directResult: Par = reducer.evalExprToPar(hexToBytesCall).unsafeRunSync
   
   val expectedResult: Par = Expr(GByteArray(ByteString.copyFrom(Base16.decode("deadbeef"))))
   directResult should be(expectedResult)

   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "eval of `toByteArray` method on any process" should "return that process serialized" in withReducer { fixtureState =>
   import fixtureState._
    
   val splitRand = rand.splitByte(0)
   import coop.rchain.models.serialization.implicits._
   val proc = Receive(Seq(ReceiveBind(Seq(ChanVar(FreeVar(0))), Quote(GString("channel")))),
                      Par(),
                      false,
                      1,
                      BitSet())
   val serializedProcess =
     com.google.protobuf.ByteString.copyFrom(Serialize[Par].encode(proc).toArray)
   val toByteArrayCall           = EMethod("toByteArray", proc, List[Par]())
   def wrapWithSend(p: Par): Par = Send(Quote(GString("result")), List[Par](p), false, BitSet())
   val env         = Env[Par]()
   val task        = reducer.eval(wrapWithSend(toByteArrayCall))(env, splitRand )
   val inspectTask = for { _ <- task } yield space.store.toMap
   val result = inspectTask.unsafeRunSync

   val channel = Channel(Quote(GString("result")))

   result should be(
     HashMap(
       List(channel) ->
         Row(List(
               Datum.create(channel,
                            ListChannelWithRandom(Seq(Quote(Expr(GByteArray(serializedProcess)))),
                                                  splitRand),
                            persist = false)),
             List())
     )
   )
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 it should "substitute before serialization" in withReducer { fixtureState =>
   import fixtureState._
    
   val splitRand = rand.splitByte(0)
   val unsubProc: Par =
     New(bindCount = 1, p = EVar(BoundVar(1)), locallyFree = BitSet(0))
   val subProc: Par =
     New(bindCount = 1, p = GPrivateBuilder("zero"), locallyFree = BitSet())
   val serializedProcess         = subProc.toByteString
   val toByteArrayCall: Par      = EMethod("toByteArray", unsubProc, List[Par](), BitSet(0))
   val channel                   = Channel(Quote(GString("result")))
   def wrapWithSend(p: Par): Par = Send(channel, List[Par](p), false, p.locallyFree)

   val env         = Env.makeEnv[Par](GPrivateBuilder("one"), GPrivateBuilder("zero"))
   val task        = reducer.eval(wrapWithSend(toByteArrayCall))(env, splitRand )
   val inspectTask = for { _ <- task } yield space.store.toMap
   val result = inspectTask.unsafeRunSync
   result should be(
     HashMap(
       List(channel) ->
         Row(List(
               Datum.create(channel,
                            ListChannelWithRandom(Seq(Quote(Expr(GByteArray(serializedProcess)))),
                                                  splitRand),
                            persist = false)),
             List())
     )
   )
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 it should "return an error when `toByteArray` is called with arguments" in withReducer { fixtureState =>
   import fixtureState._
    
   val toByteArrayWithArgumentsCall: EMethod =
     EMethod(
       "toByteArray",
       Par(sends = Seq(Send(Quote(GString("result")), List(GString("Success")), false, BitSet()))),
       List[Par](GInt(1)))

   implicit val env = Env[Par]()
   val nthTask      = reducer.eval(toByteArrayWithArgumentsCall)
   val inspectTask = for {
     _ <- nthTask
   } yield space.store.toMap
   val result = inspectTask.unsafeRunSync
   result should be(HashMap.empty)
   errorLog.readAndClearErrorVector should be(
     Vector(ReduceError("Error: toByteArray does not take arguments")))
 }

 "eval of hexToBytes" should "transform encoded string to byte array (not the rholang term)" in withReducer { fixtureState =>
   import coop.rchain.models.serialization.implicits._
   import fixtureState._
    
   val splitRand                 = rand.splitByte(0)
   val testString                = "testing testing"
   val base16Repr                = Base16.encode(testString.getBytes)
   val proc: Par                 = GString(base16Repr)
   val toByteArrayCall           = EMethod("hexToBytes", proc, List[Par]())
   def wrapWithSend(p: Par): Par = Send(Quote(GString("result")), List[Par](p), false, BitSet())
   val env         = Env[Par]()
   val task        = reducer.eval(wrapWithSend(toByteArrayCall))(env, splitRand )
   val inspectTask = for { _ <- task } yield space.store.toMap
   val result = inspectTask.unsafeRunSync

   val channel = Channel(Quote(GString("result")))

   result should be(
     HashMap(
       List(channel) ->
         Row(List(
               Datum.create(channel,
                 ListChannelWithRandom(Seq(Quote(Expr(GByteArray(ByteString.copyFrom(testString.getBytes))))), splitRand),
                 persist = false)),
             List())
     )
   )
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "variable references" should "be substituted before being used." in withReducer { fixtureState =>
   import fixtureState._
    
   val splitRandResult = rand.splitByte(3)
   val splitRandSrc    = rand.splitByte(3)
   splitRandResult.next()
   val mergeRand =
     Blake2b512Random.merge(Seq(splitRandResult.splitByte(1), splitRandResult.splitByte(0)))
   val proc = New(
     bindCount = 1,
     p = Par(
       sends = List(
         Send(chan = Channel(ChanVar(BoundVar(0))),
              data = List(EEvalBody(ChanVar(BoundVar(0)))),
              persistent = false)),
       receives = List(
         Receive(
           binds = List(
             ReceiveBind(patterns = List(Quote(Connective(VarRefBody(VarRef(0, 1))))),
                         source = ChanVar(BoundVar(0)),
                         freeCount = 0)),
           body = Send(chan = Quote(GString("result")), data = List(GString("true"))),
           bindCount = 0
         ))
     )
   )

   val env         = Env[Par]()
   val task        = reducer.eval(proc)(env, splitRandSrc )
   val inspectTask = for { _ <- task } yield space.store.toMap
   val result = inspectTask.unsafeRunSync
   val channel = Channel(Quote(GString("result")))

   result should be(
     HashMap(
       List(channel) ->
         Row(List(
               Datum.create(channel,
                            ListChannelWithRandom(Seq(Quote(GString("true"))), mergeRand),
                            persist = false)),
             List())
     )
   )

   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 it should "be substituted before being used in a match." in withReducer { fixtureState =>
   import fixtureState._
    
   val splitRandResult = rand.splitByte(4)
   val splitRandSrc    = rand.splitByte(4)
   splitRandResult.next()
   val proc = New(
     bindCount = 1,
     p = Match(
       target = EVar(BoundVar(0)),
       cases = List(
         MatchCase(pattern = Connective(VarRefBody(VarRef(0, 1))),
                   source = Send(chan = Quote(GString("result")), data = List(GString("true")))))
     )
   )


   val env         = Env[Par]()
   val task        = reducer.eval(proc)(env, splitRandSrc)
   val inspectTask = for { _ <- task } yield space.store.toMap
   val result = inspectTask.unsafeRunSync

   val channel = Channel(Quote(GString("result")))

   result should be(
     HashMap(
       List(channel) ->
         Row(List(
               Datum.create(channel,
                            ListChannelWithRandom(Seq(Quote(GString("true"))), splitRandResult),
                            persist = false)),
             List())
     )
   )

   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 it should "reference a variable that comes from a match in tuplespace" in withReducer { fixtureState =>
   import fixtureState._
    
   val baseRand   = rand.splitByte(7)
   val splitRand0 = baseRand.splitByte(0)
   val splitRand1 = baseRand.splitByte(1)
   val mergeRand  = Blake2b512Random.merge(Seq(splitRand1, splitRand0))
   val proc = Par(
     sends = List(Send(chan = Quote(GInt(7)), data = List(GInt(10)))),
     receives = List(
       Receive(
         binds = List(
           ReceiveBind(
             patterns = List(Channel(ChanVar(FreeVar(0)))),
             source = Channel(Quote(GInt(7))),
             freeCount = 1
           )),
         body = Match(
           GInt(10),
           List(
             MatchCase(
               pattern = Connective(VarRefBody(VarRef(0, 1))),
               source = Send(chan = Quote(GString("result")), data = List(GString("true")))
             ))
         )
       )
     )
   )


   val env         = Env[Par]()
   val task        = reducer.eval(proc)(env, baseRand)
   val inspectTask = for { _ <- task } yield space.store.toMap
   val result = inspectTask.unsafeRunSync
   val channel = Channel(Quote(GString("result")))

   result should be(
     HashMap(
       List(channel) ->
         Row(List(
               Datum.create(channel,
                            ListChannelWithRandom(Seq(Quote(GString("true"))), mergeRand),
                            persist = false)),
             List())
     )
   )

   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "1 matches 1" should "return true" in withReducer { fixtureState => 
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val inspectTask = reducer.evalExpr(EMatches(GInt(1), GInt(1)))
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GBool(true))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "1 matches 0" should "return false" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val inspectTask = reducer.evalExpr(EMatches(GInt(1), GInt(0)))
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GBool(false))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "1 matches _" should "return true" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val inspectTask = reducer.evalExpr(EMatches(GInt(1), EVar(Wildcard(Var.WildcardMsg()))))
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GBool(true))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "x matches 1" should "return true when x is bound to 1" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par](GInt(1))
   val inspectTask = reducer.evalExpr(EMatches(EVar(BoundVar(0)), GInt(1)))
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GBool(true))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "1 matches =x" should "return true when x is bound to 1" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par](GInt(1))
   val inspectTask = reducer.evalExpr(EMatches(GInt(1), Connective(VarRefBody(VarRef(0, 1)))))
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GBool(true))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "'abc'.length()" should "return the length of the string" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val inspectTask = reducer.evalExpr(EMethodBody(EMethod("length", GString("abc"))))
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GInt(3))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "'abcabac'.slice(3, 6)" should "return 'aba'" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val inspectTask = reducer.evalExpr(
     EMethodBody(EMethod("slice", GString("abcabac"), List(GInt(3), GInt(6))))
   )
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GString("aba"))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "'Hello, ${name}!' % {'name': 'Alice'}" should "return 'Hello, Alice!" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val inspectTask = reducer.evalExpr(
     EPercentPercentBody(
       EPercentPercent(
         GString("Hello, ${name}!"),
         EMapBody(ParMap(List[(Par, Par)]((GString("name"), GString("Alice")))))
       )
     )
   )
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GString("Hello, Alice!"))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "'abc' ++ 'def'" should "return 'abcdef" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val inspectTask = reducer.evalExpr(
     EPlusPlusBody(
       EPlusPlus(
         GString("abc"),
         GString("def")
       )
     )
   )
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GString("abcdef"))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "'${a} ${b}' % {'a': '1 ${b}', 'b': '2 ${a}'" should "return '1 ${b} 2 ${a}" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val inspectTask = reducer.evalExpr(
     EPercentPercentBody(
       EPercentPercent(
         GString("${a} ${b}"),
         EMapBody(
           ParMap(List[(Par, Par)](
                             (GString("a"), GString("1 ${b}")),
                             (GString("b"), GString("2 ${a}"))
                           ))
           )
         )
       )
     )
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GString("1 ${b} 2 ${a}"))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "[0, 1, 2, 3].length()" should "return the length of the list" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val list        = EList(List(GInt(0), GInt(1), GInt(2), GInt(3)))
   val inspectTask = reducer.evalExpr(EMethodBody(EMethod("length", list)))
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GInt(4))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "[3, 7, 2, 9, 4, 3, 7].slice(3, 5)" should "return [9, 4]" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val list = EList(List(GInt(3), GInt(7), GInt(2), GInt(9), GInt(4), GInt(3), GInt(7)))
   val inspectTask = reducer.evalExpr(
     EMethodBody(EMethod("slice", list, List(GInt(3), GInt(5))))
   )
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(EListBody(EList(List(GInt(9), GInt(4)))))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "[3, 2, 9] ++ [6, 1, 7]" should "return [3, 2, 9, 6, 1, 7]" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val lhsList = EList(List(GInt(3), GInt(2), GInt(9)))
   val rhsList = EList(List(GInt(6), GInt(1), GInt(7)))
   val inspectTask = reducer.evalExpr(
     EPlusPlusBody(
       EPlusPlus(
         lhsList,
         rhsList
       )
     )
   )
   val result = inspectTask.unsafeRunSync
   val resultList = EList(List(GInt(3), GInt(2), GInt(9), GInt(6), GInt(1), GInt(7)))
   result.exprs should be(Seq(Expr(EListBody(resultList))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "{1: 'a', 2: 'b'}.getOrElse(1, 'c')" should "return 'a'" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val map = EMapBody(
     ParMap(List[(Par, Par)]((GInt(1), GString("a")), (GInt(2), GString("b")))))
   val inspectTask = reducer.evalExpr(
     EMethodBody(EMethod("getOrElse", map, List(GInt(1), GString("c"))))
   )
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GString("a"))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "{1: 'a', 2: 'b'}.getOrElse(3, 'c')" should "return 'c'" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val map = EMapBody(
     ParMap(List[(Par, Par)]((GInt(1), GString("a")), (GInt(2), GString("b")))))
   val inspectTask = reducer.evalExpr(
     EMethodBody(EMethod("getOrElse", map, List(GInt(3), GString("c"))))
   )
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GString("c"))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "{1: 'a', 2: 'b'}.set(3, 'c')" should "return {1: 'a', 2: 'b', 3: 'c'}" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val map = EMapBody(
     ParMap(List[(Par, Par)]((GInt(1), GString("a")), (GInt(2), GString("b")))))
   val inspectTask = reducer.evalExpr(
     EMethodBody(EMethod("set", map, List(GInt(3), GString("c"))))
   )
   val result = inspectTask.unsafeRunSync
   val resultMap = EMapBody(
     ParMap(List[(Par, Par)]((GInt(1), GString("a")), (GInt(2), GString("b")), (GInt(3), GString("c")))))
   result.exprs should be(Seq(Expr(resultMap)))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "{1: 'a', 2: 'b'}.set(2, 'c')" should "return {1: 'a', 2: 'c'}" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val map = EMapBody(
     ParMap(List[(Par, Par)]((GInt(1), GString("a")), (GInt(2), GString("b")))))
   val inspectTask = reducer.evalExpr(
     EMethodBody(EMethod("set", map, List(GInt(2), GString("c")))))
   val result = inspectTask.unsafeRunSync
   val resultMap = EMapBody(
     ParMap(List[(Par, Par)]((GInt(1), GString("a")), (GInt(2), GString("c")))))
   result.exprs should be(Seq(Expr(resultMap)))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "{1: 'a', 2: 'b', 3: 'c'}.keys()" should "return Set(1, 2, 3)" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val map = EMapBody(
     ParMap(List[(Par, Par)]((GInt(1), GString("a")),
       (GInt(2), GString("b")),
       (GInt(3), GString("c")))))
   val inspectTask = reducer.evalExpr(
     EMethodBody(EMethod("keys", map))
   )
   val result = inspectTask.unsafeRunSync
   val resultSet = ESetBody(
     ParSet(
       List[Par](GInt(1), GInt(2), GInt(3))
     ))
   result.exprs should be(Seq(Expr(resultSet)))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "{1: 'a', 2: 'b', 3: 'c'}.size()" should "return 3" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val map = EMapBody(
     ParMap(List[(Par, Par)]((GInt(1), GString("a")),
                                (GInt(2), GString("b")),
                                (GInt(3), GString("c")))))
   val inspectTask = reducer.evalExpr(
     EMethodBody(EMethod("size", map))
   )
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GInt(3))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "Set(1, 2, 3).size()" should "return 3" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val set = ESetBody(ParSet(List[Par](GInt(1), GInt(2), GInt(3))))
   val inspectTask = reducer.evalExpr(
     EMethodBody(EMethod("size", set))
   )
   val result = inspectTask.unsafeRunSync
   result.exprs should be(Seq(Expr(GInt(3))))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "Set(1, 2) + 3" should "return Set(1, 2, 3)" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val set = ESetBody(ParSet(List[Par](GInt(1), GInt(2))))
   val inspectTask = reducer.evalExpr(
     EPlusBody(EPlus(set, GInt(3)))
   )
   val result = inspectTask.unsafeRunSync
   val resultSet = ESetBody(ParSet(List[Par](GInt(1), GInt(2), GInt(3))))
   result.exprs should be(Seq(Expr(resultSet)))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "{1: 'a', 2: 'b', 3: 'c'} - 3" should "return {1: 'a', 2: 'b'}" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val map = EMapBody(
     ParMap(List[(Par, Par)]((GInt(1), GString("a")),
       (GInt(2), GString("b")),
       (GInt(3), GString("c")))))
   val inspectTask = reducer.evalExpr(
       EMinusBody(EMinus(map, GInt(3)))
   )
   val result = inspectTask.unsafeRunSync
   val resultMap = EMapBody(
     ParMap(List[(Par, Par)]((GInt(1), GString("a")), (GInt(2), GString("b")))))
   result.exprs should be(Seq(Expr(resultMap)))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "Set(1, 2, 3) - 3" should "return Set(1, 2)" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val set = ESetBody(ParSet(List[Par](GInt(1), GInt(2), GInt(3))))
   val inspectTask = reducer.evalExpr(
     EMinusBody(EMinus(set, GInt(3)))
   )
   val result = inspectTask.unsafeRunSync
   val resultSet = ESetBody(ParSet(List[Par](GInt(1), GInt(2))))
   result.exprs should be(Seq(Expr(resultSet)))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "Set(1, 2) ++ Set(3, 4)" should "return Set(1, 2, 3, 4)" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val lhsSet = ESetBody(ParSet(List[Par](GInt(1), GInt(2))))
   val rhsSet = ESetBody(ParSet(List[Par](GInt(3), GInt(4))))
   val inspectTask = reducer.evalExpr(
     EPlusPlusBody(EPlusPlus(lhsSet, rhsSet))
   )
   val result = inspectTask.unsafeRunSync
   val resultSet = ESetBody(ParSet(List[Par](GInt(1), GInt(2), GInt(3), GInt(4))))
   result.exprs should be(Seq(Expr(resultSet)))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "{1: 'a', 2: 'b'} ++ {3: 'c', 4: 'd'}" should "return union" in withReducer { fixtureState =>
   import fixtureState._
    
   implicit val env = Env.makeEnv[Par]()
   val lhsMap = EMapBody(
     ParMap(List[(Par, Par)]((GInt(1), GString("a")), (GInt(2), GString("b")))))
   val rhsMap = EMapBody(
     ParMap(List[(Par, Par)]((GInt(3), GString("c")), (GInt(4), GString("d")))))
   val inspectTask = reducer.evalExpr(
     EPlusPlusBody(EPlusPlus(lhsMap, rhsMap))
   )
   val result = inspectTask.unsafeRunSync
   val resultMap = EMapBody(
     ParMap(List[(Par, Par)](
               (GInt(1), GString("a")),
               (GInt(2), GString("b")),
               (GInt(3), GString("c")),
               (GInt(4), GString("d"))
             )))
   result.exprs should be(Seq(Expr(resultMap)))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "Set(1, 2, 3, 4) -- Set(1, 2)" should "return Set(3, 4)" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val lhsSet = ESetBody(ParSet(List[Par](GInt(1), GInt(2), GInt(3), GInt(4))))
   val rhsSet = ESetBody(ParSet(List[Par](GInt(1), GInt(2))))
   val inspectTask = reducer.evalExpr(
     EMinusMinusBody(EMinusMinus(lhsSet, rhsSet))
   )
   val result = inspectTask.unsafeRunSync
   val resultSet = ESetBody(ParSet(List[Par](GInt(3), GInt(4))))
   result.exprs should be(Seq(Expr(resultSet)))
   errorLog.readAndClearErrorVector should be(Vector.empty[InterpreterError])
 }

 "Set(1, 2, 3).get(1)" should "not work" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val set         = ESetBody(ParSet(List[Par](GInt(1), GInt(2), GInt(3))))
   val inspectTask = reducer.eval(EMethodBody(EMethod("get", set, List(GInt(1)))))
   Await.ready(inspectTask.runAsync, 3.seconds)
   errorLog.readAndClearErrorVector should be(
     Vector(MethodNotDefined("get", "Set"))
   )
 }

 "{1: 'a', 2: 'b'}.add(1)" should "not work" in withReducer { fixtureState =>
   import fixtureState._
   implicit val env = Env.makeEnv[Par]()
   val map = EMapBody(
     ParMap(List[(Par, Par)]((GInt(1), GString("a")), (GInt(2), GString("b")))))
   val inspectTask = reducer.eval(EMethodBody(EMethod("add", map, List(GInt(1)))))
   Await.ready(inspectTask.runAsync, 3.seconds)
   errorLog.readAndClearErrorVector should be(
     Vector(MethodNotDefined("add", "Map"))
   )
 }
}
