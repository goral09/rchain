package coop.rchain.rholang.interpreter.storage
import cats.effect.Sync
import com.google.protobuf.ByteString
import coop.rchain.crypto.hash.Blake2b512Random
import coop.rchain.models.Channel.ChannelInstance.Quote
import coop.rchain.models.Expr.ExprInstance.GInt
import coop.rchain.models.TaggedContinuation.TaggedCont.ParBody
import coop.rchain.models._
import coop.rchain.models.rholang.implicits._
import coop.rchain.rholang.interpreter.Runtime.RhoPureSpace
import coop.rchain.rholang.interpreter.accounting.{CostAccount, CostAccountingAlg, _}
import coop.rchain.rholang.interpreter.errors.OutOfPhlogistonsError
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalactic.TripleEqualsSupport
import org.scalatest.{fixture, Matchers, Outcome}

import scala.collection.immutable.{Seq => ImmSeq}
import scala.concurrent.Await
import scala.concurrent.duration._

class ChargingRSpaceTest extends fixture.FlatSpec with TripleEqualsSupport with Matchers {

  behavior of "ChargingRSpace"

  private def byteName(b: Byte): GPrivate = GPrivate(ByteString.copyFrom(Array[Byte](b)))

  val rand: Blake2b512Random = Blake2b512Random(Array.empty[Byte])
  val RSPACE_MATCH_COST      = CostAccount.fromProto(TestISpace.RSPACE_MATCH_PCOST)

  it should "charge for storing data in tuplespace" in { fixture =>
    val TestFixture(chargingRSpace, costAlg, pureRSpace) = fixture
    // format: off
    val channels = ImmSeq(Quote(byteName(10))).map(Channel(_))
    val patterns = ImmSeq(BindPattern(Vector(Channel(Quote(Par().copy(exprs = Vector(Expr(GInt(1))))))), None, 0))
    val continuation = TaggedContinuation(ParBody(ParWithRandom(Par(), rand)))
    // format: on
    val storageCost  = ChargingRSpace.storageCostConsume(channels, patterns, continuation)
    val minimumPhlos = storageCost + Cost(1)
    Await.ready(costAlg.set(CostAccount(0, minimumPhlos)).runAsync, 1.second)

    val test = for {
      _ <- chargingRSpace.consume(channels, patterns, continuation, false)
      _ = pureRSpace.consumesMap.get(channels.head) shouldBe Some(
        List(Consume(channels, patterns, continuation, false)))
      phlosLeft <- costAlg.get()
      // we expect Cost = 1 because there will be no match
      // so we will pay only for the storage
      _ = phlosLeft.cost shouldBe Cost(1)
    } yield ()

    Await.result(test.runAsync, 1.second)
  }

  it should "refund if data doesn't stay in tuplespace" in { fixture =>
    val TestFixture(chargingRSpace, costAlg, pureRSpace) = fixture
    // format: off
    val channels = ImmSeq(Quote(byteName(10))).map(Channel(_))
    val patterns = ImmSeq(BindPattern(Vector(Channel(Quote(Par().copy(exprs = Vector(Expr(GInt(1))))))), None, 0))
    val continuation = TaggedContinuation(ParBody(ParWithRandom(Par(), rand)))
    // format: on
    val consumeStorageCost = ChargingRSpace.storageCostConsume(channels, patterns, continuation)

    val data               = ListChannelWithRandom()
    val produceStorageCost = ChargingRSpace.storageCostProduce(channels.head, data)
    val minimumPhlos       = produceStorageCost + consumeStorageCost + RSPACE_MATCH_COST.cost + Cost(1)
    Await.ready(costAlg.set(CostAccount(0, minimumPhlos)).runAsync, 1.second)

    val test = for {
      _ <- chargingRSpace.produce(channels.head, data, false)
      _ = pureRSpace.producesMap.get(channels.head) shouldBe Some(
        List(Produce(channels.head, data, false)))
      phlosAfterProduce <- costAlg.get()
      _                 = phlosAfterProduce.cost shouldBe (minimumPhlos - produceStorageCost)
      _                 <- chargingRSpace.consume(channels, patterns, continuation, false)
      // after the COMM happens we should not remove it from the Map
      _ = pureRSpace.producesMap.get(channels.head) shouldBe Some(List.empty)
      // because we never got to store the consume in the map there won't be an entry in the Map
      _         = pureRSpace.consumesMap.get(channels.head) shouldBe None
      phlosLeft <- costAlg.get()
      // we expect Cost(16), because we will be refunded for storing the consume
      _ = phlosLeft.cost shouldBe (consumeStorageCost + Cost(1))
    } yield ()

    Await.result(test.runAsync, 1.second)
  }

  it should "fail with OutOfPhloError when deploy runs out of it" in { fixture =>
    val TestFixture(chargingRSpace, costAlg, pureRSpace) = fixture
    val channel                                          = Channel(Quote(byteName(1)))
    val data                                             = ListChannelWithRandom()
    val produceStorageCost                               = ChargingRSpace.storageCostProduce(channel, data)

    Await.ready(costAlg.set(CostAccount(0, produceStorageCost - Cost(1))).runAsync, 1.second)

    val test = for {
      _ <- chargingRSpace.produce(channel, data, false)
    } yield ()

    val outOfPhloTest = Await.result(test.attempt.runAsync, 1.second)
    assert(outOfPhloTest === Left(OutOfPhlogistonsError))

    val costAlgTest = Await.result(costAlg.get().runAsync, 1.second)
    assert(costAlgTest === CostAccount(1, Cost(-1)))
  }

  type ChargingRSpace = RhoPureSpace[Task]

  override protected def withFixture(test: OneArgTest): Outcome = {
    implicit val costAlg    = CostAccountingAlg.unsafe[Task](CostAccount(0))
    implicit val pureRSpace = TestISpace()
    implicit val s          = implicitly[Sync[Task]]
    val chargingRSpace      = ChargingRSpace.pureRSpace(s, costAlg, pureRSpace)
    test(TestFixture(chargingRSpace, costAlg, pureRSpace))
  }
  final case class TestFixture(chargingRSpace: ChargingRSpace,
                               costAlg: CostAccountingAlg[Task],
                               pureRSpace: TestISpace)

  override type FixtureParam = TestFixture
}
