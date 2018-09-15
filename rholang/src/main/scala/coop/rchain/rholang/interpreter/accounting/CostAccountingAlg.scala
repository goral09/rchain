package coop.rchain.rholang.interpreter.accounting

import cats.{FlatMap, Monad}
import cats.effect.Sync
import cats.effect.concurrent.Ref
import coop.rchain.rholang.interpreter.accounting
import cats.implicits._
import coop.rchain.rholang.interpreter.errors.OutOfPhlogistonsError

trait CostAccountingAlg[F[_]] {
  def charge(cost: Cost): F[Unit]
  def get(): F[CostAccount]
  def set(cost: CostAccount): F[Unit]
}

object CostAccountingAlg {
  def apply[F[_]: Sync](init: CostAccount): F[CostAccountingAlg[F]] = Ref[F].of(init).map { state =>
    new CostAccountingAlg[F] {
      override def charge(cost: Cost): F[Unit] =
        for {
          _ <- failOnOutOfPhlo
          _ <- state.update(_ - cost)
          _ <- failOnOutOfPhlo
        } yield ()
      override def get: F[CostAccount]             = state.get
      override def set(cost: CostAccount): F[Unit] = state.set(cost)

      private val failOnOutOfPhlo: F[Unit] =
        FlatMap[F].ifM(state.get.map(_.cost.value < 0))(Sync[F].raiseError(OutOfPhlogistonsError),
                                                        Sync[F].unit)
    }
  }

  def of[F[_]](implicit ev: CostAccountingAlg[F]): CostAccountingAlg[F] = ev

  def unsafe[F[_]: Monad](initialState: CostAccount)(implicit F: Sync[F]): CostAccountingAlg[F] = {
    val state = Ref.unsafe[F, CostAccount](initialState)
    new CostAccountingAlg[F] {

      override def set(cost: CostAccount): F[Unit] =
        state.set(cost)

      override def charge(cost: accounting.Cost): F[Unit] =
        state.update(_ - cost)

      override def get(): F[CostAccount] = state.get
    }
  }
}
