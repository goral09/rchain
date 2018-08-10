package coop.rchain.blockstorage

import scala.language.higherKinds

import cats._
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import coop.rchain.blockstorage.BlockStore.BlockHash
import coop.rchain.casper.protocol.BlockMessage
import coop.rchain.metrics.Metrics

class InMemBlockStore[F[_], E] private ()(implicit
                                          monadF: Monad[F],
                                          refF: Ref[F, Map[BlockHash, BlockMessage]],
                                          metricsF: Metrics[F])
    extends BlockStore[F] {

  def get(blockHash: BlockHash): F[Option[BlockMessage]] =
    for {
      _     <- metricsF.incrementCounter("block-store-get")
      state <- refF.get
    } yield state.get(blockHash)

  @deprecated(message = "to be removed when casper code no longer needs the whole DB in memmory",
              since = "0.5")
  def asMap(): F[Map[BlockHash, BlockMessage]] =
    for {
      _     <- metricsF.incrementCounter("block-store-as-map")
      state <- refF.get
    } yield state

  def put(f: => (BlockHash, BlockMessage)): F[Unit] =
    for {
      _ <- metricsF.incrementCounter("block-store-put")
      _ <- refF.update { state =>
            val (hash, message) = f
            state.updated(hash, message)
          }
    } yield ()

  def clear(): F[Unit] =
    for {
      _ <- refF.update { _.empty }
    } yield ()

  override def close(): F[Unit] = monadF.pure(())
}

object InMemBlockStore {
  def create[F[_], E](implicit
                      monadF: Monad[F],
                      refF: Ref[F, Map[BlockHash, BlockMessage]],
                      metricsF: Metrics[F]): BlockStore[F] =
    new InMemBlockStore()

  def createWithF[F[_]: Sync]: BlockStore[F] = {
    import coop.rchain.metrics.Metrics.MetricsNOP
    import coop.rchain.catscontrib.effect.implicits._
    implicit val refId               = emptyMapRef[F]
    implicit val metrics: Metrics[F] = new MetricsNOP[F]()
    InMemBlockStore.create
  }

  def emptyMapRef[F[_]](implicit syncEv: Sync[F]): Ref[F, Map[BlockHash, BlockMessage]] =
    Ref.unsafe(Map.empty[BlockHash, BlockMessage])

}
