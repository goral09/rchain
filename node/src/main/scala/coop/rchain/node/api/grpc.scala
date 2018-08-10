package coop.rchain.node.api

import cats._
import cats.implicits._
import coop.rchain.blockstorage.BlockStore
import coop.rchain.casper.protocol.DeployServiceGrpc
import coop.rchain.casper.{MultiParentCasper, SafetyOracle}
import coop.rchain.catscontrib._
import coop.rchain.comm.discovery._
import coop.rchain.node.diagnostics
import coop.rchain.node.diagnostics.{JvmMetrics, NodeMetrics}
import coop.rchain.node.model.diagnostics._
import coop.rchain.node.model.repl._
import coop.rchain.rholang.interpreter.Runtime
import coop.rchain.shared._
import io.grpc.{Server, ServerBuilder}
import monix.execution.Scheduler

object GrpcServer {

  private implicit val logSource: LogSource = LogSource(this.getClass)

  def acquireServer[
      F[_]: Capture: Monad: MultiParentCasper: Log: NodeDiscovery: JvmMetrics: NodeMetrics: Futurable: SafetyOracle: BlockStore](
      port: Int,
      runtime: Runtime)(implicit scheduler: Scheduler): F[Server] =
    Capture[F].capture {
      ServerBuilder
        .forPort(port)
        .addService(ReplGrpc.bindService(new ReplGrpcService(runtime), scheduler))
        .addService(DiagnosticsGrpc.bindService(diagnostics.grpc[F], scheduler))
        .addService(DeployServiceGrpc.bindService(new DeployGrpcService[F], scheduler))
        .build
    }

  def start[F[_]: FlatMap: Capture: Log](server: Server): F[Unit] =
    for {
      _ <- Capture[F].capture(server.start)
      _ <- Log[F].info("gRPC server started, listening on ")
    } yield ()

}
