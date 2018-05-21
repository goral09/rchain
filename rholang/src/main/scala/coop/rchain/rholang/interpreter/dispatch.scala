package coop.rchain.rholang.interpreter

import cats.Applicative
import coop.rchain.catscontrib.Capture
import coop.rchain.models.Channel.ChannelInstance.Quote
import coop.rchain.models.TaggedContinuation.TaggedCont.{Empty, ParBody, ScalaBodyRef}
import coop.rchain.models.{BindPattern, Channel, Par, TaggedContinuation}
import coop.rchain.rholang.interpreter.errors._
import coop.rchain.rspace.IStore

trait Dispatch[M[_], A, K] {

  val reducer: Reduce[M]

  def dispatch(continuation: K, dataList: Seq[A]): M[Unit]
}

object Dispatch {

  // TODO: Make this function total
  def buildEnv(dataList: Seq[Seq[Channel]]): Env[Par] =
    Env.makeEnv(dataList.flatten.map({ case Channel(Quote(p)) => p }): _*)

  //TODO(mateusz.gorski): Move to test directory
  def rholangOnlyDispatcher[M[_]: Applicative: Capture: InterpreterErrorsM, F[_]](
      tuplespace: IStore[Channel, BindPattern, Seq[Channel], TaggedContinuation])(
      implicit parallel: cats.Parallel[M, F]): Dispatch[M, Seq[Channel], TaggedContinuation] = {
    lazy val dispatcher: Dispatch[M, Seq[Channel], TaggedContinuation] =
      new Dispatch[M, Seq[Channel], TaggedContinuation] {
        val reducer: Reduce[M] = _reducer

        def dispatch(continuation: TaggedContinuation, dataList: Seq[Seq[Channel]]): M[Unit] =
          continuation.taggedCont match {
            case ParBody(par) =>
              val env = Dispatch.buildEnv(dataList)
              reducer.eval(par)(env)
            case ScalaBodyRef(_) =>
              Applicative[M].pure(Unit)
            case Empty =>
              Applicative[M].pure(Unit)
          }
      }

    lazy val _reducer: Reduce[M] =
      new Reduce.DebruijnInterpreter[M, F](tuplespace, dispatcher)

    dispatcher
  }

  def rholangAndScalaDispatcher[M[_]: Applicative: Capture: InterpreterErrorsM, F[_]](
      tuplespace: IStore[Channel, BindPattern, Seq[Channel], TaggedContinuation],
      dispatchTable: => Map[Long, Function1[Seq[Seq[Channel]], M[Unit]]])(
      implicit parallel: cats.Parallel[M, F]): Dispatch[M, Seq[Channel], TaggedContinuation] = {

    lazy val dispatcher: Dispatch[M, Seq[Channel], TaggedContinuation] =
      new Dispatch[M, Seq[Channel], TaggedContinuation] {
        val reducer: Reduce[M] = _reducer

        def dispatch(continuation: TaggedContinuation, dataList: Seq[Seq[Channel]]): M[Unit] =
          continuation.taggedCont match {
            case ParBody(par) =>
              val env = Dispatch.buildEnv(dataList)
              reducer.eval(par)(env)
            case ScalaBodyRef(_) =>
              Applicative[M].pure(Unit)
            case Empty =>
              Applicative[M].pure(Unit)
          }
      }

    lazy val _reducer: Reduce[M] =
      new Reduce.DebruijnInterpreter[M, F](tuplespace, dispatcher)

    dispatcher

  }
}
