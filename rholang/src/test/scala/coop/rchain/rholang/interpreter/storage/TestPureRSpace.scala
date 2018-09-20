package coop.rchain.rholang.interpreter.storage

import coop.rchain.models._
import coop.rchain.rholang.interpreter.Runtime.RhoPureSpace
import coop.rchain.rholang.interpreter.errors
import coop.rchain.rholang.interpreter.storage.TestPureRSpace._
import coop.rchain.rspace
import coop.rchain.rspace.{Blake2b256Hash, Checkpoint}
import monix.eval.Task

import scala.collection.immutable.{Seq => ImmSeq}
import scala.collection.mutable
import scala.collection.mutable.Map

final case class Consume(channels: ImmSeq[Channel],
                         patterns: ImmSeq[BindPattern],
                         continuation: TaggedContinuation,
                         persist: Boolean)
final case class Produce(channel: Channel, data: ListChannelWithRandom, persist: Boolean)

object TestPureRSpace {
  val RSPACE_MATCH_PCOST: PCost = PCost(1, 100)
  def apply(): TestPureRSpace   = new TestPureRSpace(mutable.HashMap.empty, mutable.HashMap.empty)
}

class TestPureRSpace(val consumesMap: Map[Channel, List[Consume]],
                     val producesMap: Map[Channel, List[Produce]])
    extends RhoPureSpace[Task] {
  override def consume(channels: ImmSeq[Channel],
                       patterns: ImmSeq[BindPattern],
                       continuation: TaggedContinuation,
                       persist: Boolean)(implicit m: rspace.Match[BindPattern,
                                                                  errors.OutOfPhlogistonsError.type,
                                                                  ListChannelWithRandom,
                                                                  ListChannelWithRandom])
    : Task[Either[errors.OutOfPhlogistonsError.type,
                  Option[(TaggedContinuation, ImmSeq[ListChannelWithRandom])]]] = {
    val cs = Consume(channels, patterns, continuation, persist)
    def updateConsumesMap: Unit = {
      val soFar = consumesMap.getOrElse(channels.head, List.empty[Consume])
      val newCs = cs :: soFar
      consumesMap.update(channels.head, newCs)
    }
    producesMap.get(channels.head) match {
      case None =>
        updateConsumesMap
        Task.now(Right(None))
      case Some(produces) =>
        val h :: tl = produces
        val newPrs  = if (h.persist) produces else tl
        producesMap.update(channels.head, newPrs)
        if (persist) updateConsumesMap
        val channelsData = ListChannelWithRandom().withCost(RSPACE_MATCH_PCOST)
        Task.now(Right(Some((continuation, List(channelsData)))))
    }
  }
  override def produce(channel: Channel, data: ListChannelWithRandom, persist: Boolean)(
      implicit m: rspace.Match[BindPattern,
                               errors.OutOfPhlogistonsError.type,
                               ListChannelWithRandom,
                               ListChannelWithRandom])
    : Task[Either[errors.OutOfPhlogistonsError.type,
                  Option[(TaggedContinuation, ImmSeq[ListChannelWithRandom])]]] = {
    val produce = Produce(channel, data, persist)
    def updateProducesMap: Unit = {
      val soFar = producesMap.getOrElse(channel, List.empty[Produce])
      val newPr = produce :: soFar
      producesMap.update(channel, newPr)
    }

    consumesMap.get(channel) match {
      case None =>
        updateProducesMap
        Task.now(Right(None))
      case Some(consumes) =>
        val h :: tl = consumes
        val newCs   = if (h.persist) consumes else tl
        if (persist) updateProducesMap
        val channelsData = ListChannelWithRandom().withCost(RSPACE_MATCH_PCOST)
        Task.now(Right(Some((h.continuation, List(channelsData)))))
    }
  }

  // We don't care about these in test
  override def install(channels: ImmSeq[Channel],
                       patterns: ImmSeq[BindPattern],
                       continuation: TaggedContinuation)(
      implicit m: rspace.Match[BindPattern,
                               errors.OutOfPhlogistonsError.type,
                               ListChannelWithRandom,
                               ListChannelWithRandom])
    : Task[Option[(TaggedContinuation, ImmSeq[ListChannelWithRandom])]] =
    ???
  override def createCheckpoint(): Task[Checkpoint] =
    ???
  override def reset(hash: Blake2b256Hash): Task[Unit] = ???
  override def close(): Task[Unit]                     = ???
}
