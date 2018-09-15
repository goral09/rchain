package coop.rchain.rholang.interpreter

import java.nio.file.{Files, Path}

import cats.mtl.FunctorTell
import com.google.protobuf.ByteString
import coop.rchain.models.Channel.ChannelInstance.{ChanVar, Quote}
import coop.rchain.models.Expr.ExprInstance.GString
import coop.rchain.models.TaggedContinuation.TaggedCont.ScalaBodyRef
import coop.rchain.models.Var.VarInstance.FreeVar
import coop.rchain.models._
import coop.rchain.models.rholang.implicits._
import coop.rchain.rholang.interpreter.Runtime._
import coop.rchain.rholang.interpreter.accounting.{CostAccount, CostAccountingAlg}
import coop.rchain.rholang.interpreter.errors.OutOfPhlogistonsError
import coop.rchain.rholang.interpreter.storage.TuplespaceAlg
import coop.rchain.rholang.interpreter.storage.implicits._
import coop.rchain.rspace._
import coop.rchain.rspace.history.Branch
import coop.rchain.rspace.pure.PureRSpace
import coop.rchain.shared.StoreType
import coop.rchain.shared.StoreType._
import monix.eval.Task

import scala.collection.immutable

class Runtime private (val reducer: ChargingReducer[Task],
                       val replayReducer: ChargingReducer[Task],
                       val space: RhoISpace,
                       val replaySpace: RhoReplayRSpace,
                       var errorLog: ErrorLog,
                       val context: RhoContext) {
  def readAndClearErrorVector(): Vector[Throwable] = errorLog.readAndClearErrorVector()
  def close(): Unit = {
    space.close()
    replaySpace.close()
    context.close()
  }
}

object Runtime {

  type RhoISpace       = CPARK[FreudianSpace]
  type RhoRSpace       = CPARK[RSpace]
  type RhoReplayRSpace = CPARK[ReplayRSpace]

  type RhoIStore  = CPAK[IStore]
  type RhoContext = CPAK[Context]

  type CPAK[F[_, _, _, _]] =
    F[Channel, BindPattern, ListChannelWithRandom, TaggedContinuation]

  type CPARK[F[_, _, _, _, _, _]] =
    F[Channel,
      BindPattern,
      OutOfPhlogistonsError.type,
      ListChannelWithRandom,
      ListChannelWithRandom,
      TaggedContinuation]

  type Name      = Par
  type Arity     = Int
  type Remainder = Option[Var]
  type Ref       = Long

  private def introduceSystemProcesses(space: RhoISpace,
                                       replaySpace: RhoISpace,
                                       processes: immutable.Seq[(Name, Arity, Remainder, Ref)])
    : Seq[Option[(TaggedContinuation, Seq[ListChannelWithRandom])]] =
    processes.flatMap {
      case (name, arity, remainder, ref) =>
        val channels = List(Channel(Quote(name)))
        val patterns = List(
          BindPattern((0 until arity).map[Channel, Seq[Channel]](i => ChanVar(FreeVar(i))),
                      remainder,
                      freeCount = arity))
        val continuation = TaggedContinuation(ScalaBodyRef(ref))
        Seq(
          space.install(channels, patterns, continuation),
          replaySpace.install(channels, patterns, continuation)
        )
    }

  // TODO: remove default store type
  def create(dataDir: Path, mapSize: Long, storeType: StoreType = LMDB): Runtime = {
    val context: RhoContext = storeType match {
      case InMem => Context.createInMemory()
      case LMDB =>
        if (Files.notExists(dataDir)) {
          Files.createDirectories(dataDir)
        }
        Context.create(dataDir, mapSize, true)
      case Mixed =>
        if (Files.notExists(dataDir)) {
          Files.createDirectories(dataDir)
        }
        Context.createMixed(dataDir, mapSize)
    }

    val space: RhoRSpace             = RSpace.create(context, Branch.MASTER)
    val replaySpace: RhoReplayRSpace = ReplayRSpace.create(context, Branch.REPLAY)

    val errorLog                                  = new ErrorLog()
    implicit val ft: FunctorTell[Task, Throwable] = errorLog

    def dispatchTableCreator(
        space: RhoISpace,
        dispatcher: Dispatch[Task, ListChannelWithRandom, TaggedContinuation]) = Map(
      0L -> SystemProcesses.stdout,
      1L -> SystemProcesses.stdoutAck(space, dispatcher),
      2L -> SystemProcesses.stderr,
      3L -> SystemProcesses.stderrAck(space, dispatcher),
      4L -> SystemProcesses.ed25519Verify(space, dispatcher),
      5L -> SystemProcesses.sha256Hash(space, dispatcher),
      6L -> SystemProcesses.keccak256Hash(space, dispatcher),
      7L -> SystemProcesses.blake2b256Hash(space, dispatcher),
      9L -> SystemProcesses.secp256k1Verify(space, dispatcher)
    )

    def byteName(b: Byte): Par = GPrivate(ByteString.copyFrom(Array[Byte](b)))

    val urnMap: Map[String, Par] = Map("rho:io:stdout" -> byteName(0),
                                       "rho:io:stdoutAck" -> byteName(1),
                                       "rho:io:stderr"    -> byteName(2),
                                       "rho:io:stderrAck" -> byteName(3))

    // replay
    lazy val replayPureSpace = PureRSpace[Task].of(replaySpace)
    lazy val replayDispatchTable: Map[Ref, Seq[ListChannelWithRandom] => Task[Unit]] =
      dispatchTableCreator(replaySpace, replayDispatcher)
    lazy val replayTuplespaceAlg =
      TuplespaceAlg.rspaceTuplespace[Task, Task.Par](replayPureSpace, replayDispatcher)
    lazy val replayCostAccounting = CostAccountingAlg.unsafe[Task](CostAccount.zero)
    lazy val replayReducer: ChargingReducer[Task] =
      new Reducer.DebruijnInterpreter[Task, Task.Par](replayTuplespaceAlg,
                                                      replayCostAccounting,
                                                      urnMap)
    lazy val replayDispatcher: Dispatch[Task, ListChannelWithRandom, TaggedContinuation] =
      RholangAndScalaDispatcher.create(replayReducer, replayDispatchTable)

    // pure
    lazy val pureSpace = PureRSpace[Task].of(space)
    lazy val dispatchTable: Map[Ref, Seq[ListChannelWithRandom] => Task[Unit]] =
      dispatchTableCreator(space, dispatcher)
    lazy val tuplespaceAlg  = TuplespaceAlg.rspaceTuplespace[Task, Task.Par](pureSpace, dispatcher)
    lazy val costAccounting = CostAccountingAlg.unsafe[Task](CostAccount.zero)
    lazy val reducer: ChargingReducer[Task] =
      new Reducer.DebruijnInterpreter[Task, Task.Par](tuplespaceAlg, costAccounting, urnMap)
    lazy val dispatcher: Dispatch[Task, ListChannelWithRandom, TaggedContinuation] =
      RholangAndScalaDispatcher.create(reducer, dispatchTable)

    val procDefs: immutable.Seq[(Name, Arity, Remainder, Ref)] = List(
      (byteName(0), 1, None, 0L),
      (byteName(1), 2, None, 1L),
      (byteName(2), 1, None, 2L),
      (byteName(3), 2, None, 3L),
      (GString("ed25519Verify"), 4, None, 4L),
      (GString("sha256Hash"), 2, None, 5L),
      (GString("keccak256Hash"), 2, None, 6L),
      (GString("blake2b256Hash"), 2, None, 7L),
      (GString("secp256k1Verify"), 4, None, 9L)
    )

    val res: Seq[Option[(TaggedContinuation, Seq[ListChannelWithRandom])]] =
      introduceSystemProcesses(space, replaySpace, procDefs)

    assert(res.forall(_.isEmpty))

    new Runtime(reducer, replayReducer, space, replaySpace, errorLog, context)
  }
}
