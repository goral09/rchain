package coop.rchain.rholang.interpreter.storage

import cats.Id
import coop.rchain.models._
import coop.rchain.rholang.interpreter.Runtime.RhoISpace
import coop.rchain.rholang.interpreter.errors
import coop.rchain.rholang.interpreter.storage.TestISpace._
import coop.rchain.rspace
import coop.rchain.rspace.{internal, Blake2b256Hash, Checkpoint, IStore}

import scala.collection.immutable.{Seq => ImmSeq}
import scala.collection.mutable
import scala.collection.mutable.Map

final case class Consume(channels: ImmSeq[Channel],
                         patterns: ImmSeq[BindPattern],
                         continuation: TaggedContinuation,
                         persist: Boolean)
final case class Produce(channel: Channel, data: ListChannelWithRandom, persist: Boolean)

object TestISpace {
  val RSPACE_MATCH_PCOST: PCost = PCost(1, 100)
  def apply(): TestISpace       = new TestISpace(mutable.HashMap.empty, mutable.HashMap.empty)
}

class TestISpace(val consumesMap: Map[Channel, List[Consume]],
                 val producesMap: Map[Channel, List[Produce]])
    extends RhoISpace {

  override def consume(channels: ImmSeq[Channel],
                       patterns: ImmSeq[BindPattern],
                       continuation: TaggedContinuation,
                       persist: Boolean)(implicit m: rspace.Match[BindPattern,
                                                                  errors.OutOfPhlogistonsError.type,
                                                                  ListChannelWithRandom,
                                                                  ListChannelWithRandom])
    : Id[Either[errors.OutOfPhlogistonsError.type,
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
        Right(None)
      case Some(produces) =>
        val h :: tl = produces
        val newPrs  = if (h.persist) produces else tl
        producesMap.update(channels.head, newPrs)
        if (persist) updateConsumesMap
        val channelsData = ListChannelWithRandom().withCost(RSPACE_MATCH_PCOST)
        Right(Some((continuation, List(channelsData))))
    }
  }
  override def install(channels: ImmSeq[Channel],
                       patterns: ImmSeq[BindPattern],
                       continuation: TaggedContinuation)(
      implicit m: rspace.Match[BindPattern,
                               errors.OutOfPhlogistonsError.type,
                               ListChannelWithRandom,
                               ListChannelWithRandom])
    : Id[Option[(TaggedContinuation, ImmSeq[ListChannelWithRandom])]] = ???

  override def produce(channel: Channel, data: ListChannelWithRandom, persist: Boolean)(
      implicit m: rspace.Match[BindPattern,
                               errors.OutOfPhlogistonsError.type,
                               ListChannelWithRandom,
                               ListChannelWithRandom])
    : Id[Either[errors.OutOfPhlogistonsError.type,
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
        Right(None)
      case Some(consumes) =>
        val h :: tl = consumes
        val newCs   = if (h.persist) consumes else tl
        if (persist) updateProducesMap
        val channelsData = ListChannelWithRandom().withCost(RSPACE_MATCH_PCOST)
        Right(Some((h.continuation, List(channelsData))))
    }
  }

  override def retrieve(root: Blake2b256Hash, channelsHash: Blake2b256Hash)
    : Id[Option[internal.GNAT[Channel, BindPattern, ListChannelWithRandom, TaggedContinuation]]] =
    ???
  override def getData(channel: Channel): ImmSeq[internal.Datum[ListChannelWithRandom]] = ???
  override def getWaitingContinuations(channels: ImmSeq[Channel])
    : ImmSeq[internal.WaitingContinuation[BindPattern, TaggedContinuation]] = ???

  override def clear(): Id[Unit] = ???

  override def createCheckpoint(): Id[Checkpoint] = ???

  override def reset(root: Blake2b256Hash): Id[Unit] = ???

  override def close(): Id[Unit]                                                              = ???
  override val store: IStore[Channel, BindPattern, ListChannelWithRandom, TaggedContinuation] = null

}
