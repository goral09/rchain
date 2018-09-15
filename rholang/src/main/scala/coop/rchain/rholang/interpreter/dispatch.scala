package coop.rchain.rholang.interpreter

import cats.Parallel
import cats.effect.Sync
import cats.implicits._
import cats.mtl.FunctorTell
import coop.rchain.crypto.hash.Blake2b512Random
import coop.rchain.models.Channel.ChannelInstance.Quote
import coop.rchain.models.TaggedContinuation.TaggedCont.{Empty, ParBody, ScalaBodyRef}
import coop.rchain.models._

trait Dispatch[M[_], A, K] {
  def dispatch(continuation: K, dataList: Seq[A]): M[Unit]
}

object Dispatch {

  // TODO: Make this function total
  def buildEnv(dataList: Seq[ListChannelWithRandom]): Env[Par] =
    Env.makeEnv(
      dataList
        .flatMap(_.channels)
        .map({
          case Channel(Quote(p)) => p
          case Channel(_)        => Par() // Should never happen
        }): _*)
}

class RholangOnlyDispatcher[M[_]] private (reducer: => ChargingReducer[M])(implicit s: Sync[M])
    extends Dispatch[M, ListChannelWithRandom, TaggedContinuation] {

  def dispatch(continuation: TaggedContinuation, dataList: Seq[ListChannelWithRandom]): M[Unit] =
    for {
      //TODO: update phlos lefts
      res <- continuation.taggedCont match {
              case ParBody(parWithRand) =>
                val env     = Dispatch.buildEnv(dataList)
                val randoms = parWithRand.randomState +: dataList.toVector.map(_.randomState)
                reducer.eval(parWithRand.body)(env, Blake2b512Random.merge(randoms))
              case ScalaBodyRef(_) =>
                s.unit
              case Empty =>
                s.unit
            }
    } yield res
}

object RholangOnlyDispatcher {

  def create[M[_]: Sync, F[_]](reducer: => ChargingReducer[M], urnMap: Map[String, Par] = Map.empty)
    : Dispatch[M, ListChannelWithRandom, TaggedContinuation] = {
    lazy val dispatcher: Dispatch[M, ListChannelWithRandom, TaggedContinuation] =
      new RholangOnlyDispatcher(reducer)
    dispatcher
  }
}

class RholangAndScalaDispatcher[M[_]] private (
    reducer: => ChargingReducer[M],
    _dispatchTable: => Map[Long, Function1[Seq[ListChannelWithRandom], M[Unit]]])(
    implicit s: Sync[M])
    extends Dispatch[M, ListChannelWithRandom, TaggedContinuation] {

  def dispatch(continuation: TaggedContinuation, dataList: Seq[ListChannelWithRandom]): M[Unit] =
    for {
      //TODO: update phlos left
      res <- continuation.taggedCont match {
              case ParBody(parWithRand) =>
                val env     = Dispatch.buildEnv(dataList)
                val randoms = parWithRand.randomState +: dataList.toVector.map(_.randomState)
                reducer.eval(parWithRand.body)(env, Blake2b512Random.merge(randoms))
              case ScalaBodyRef(ref) =>
                _dispatchTable.get(ref) match {
                  case Some(f) => f(dataList)
                  case None    => s.raiseError(new Exception(s"dispatch: no function for $ref"))
                }
              case Empty =>
                s.unit
            }
    } yield res

}

object RholangAndScalaDispatcher {

  def create[M[_], F[_]](
      reducer: => ChargingReducer[M],
      dispatchTable: => Map[Long, Function1[Seq[ListChannelWithRandom], M[Unit]]])(
      implicit
      s: Sync[M]): Dispatch[M, ListChannelWithRandom, TaggedContinuation] = {

    lazy val dispatcher: Dispatch[M, ListChannelWithRandom, TaggedContinuation] =
      new RholangAndScalaDispatcher(reducer, dispatchTable)

    dispatcher
  }
}
