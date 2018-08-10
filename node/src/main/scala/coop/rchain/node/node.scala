package coop.rchain.node

import cats._
import cats.data._
import cats.effect.Timer
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._
import coop.rchain.blockstorage.{BlockStore, LMDBBlockStore}
import coop.rchain.casper.protocol.ApprovedBlock
import coop.rchain.casper.util.comm.CommUtil.{casperPacketHandler, requestApprovedBlock}
import coop.rchain.casper.util.rholang.RuntimeManager
import coop.rchain.casper._
import coop.rchain.catscontrib.Catscontrib._
import coop.rchain.catscontrib.TaskContrib._
import coop.rchain.catscontrib._
import coop.rchain.comm.CommError.ErrorHandler
import coop.rchain.comm._
import coop.rchain.comm.discovery._
import coop.rchain.comm.protocol.rchain.Packet
import coop.rchain.comm.protocol.routing._
import coop.rchain.comm.rp.Connect.ConnectionsCell
import coop.rchain.comm.rp._
import coop.rchain.comm.transport._
import coop.rchain.crypto.codec.Base16
import coop.rchain.metrics.Metrics
import coop.rchain.node.api._
import coop.rchain.node.configuration.Configuration
import coop.rchain.node.diagnostics.{MetricsServer, _}
import coop.rchain.p2p.effects._
import coop.rchain.rholang.interpreter.Runtime
import coop.rchain.shared.ThrowableOps._
import coop.rchain.shared._
import io.grpc.Server
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS, TimeUnit}
import scala.util.{Failure, Success, Try}

class NodeRuntime(conf: Configuration, host: String)(implicit scheduler: Scheduler) {

  private implicit val logSource: LogSource = LogSource(this.getClass)

  private val dataDirFile = conf.server.dataDir.toFile

  if (!dataDirFile.exists()) {
    if (!dataDirFile.mkdir()) {
      println(
        s"The data dir must be a directory and have read and write permissions:\n${dataDirFile.getAbsolutePath}")
      System.exit(-1)
    }
  }

  // Check if data_dir has read/write access
  if (!dataDirFile.isDirectory || !dataDirFile.canRead || !dataDirFile.canWrite) {
    println(
      s"The data dir must be a directory and have read and write permissions:\n${dataDirFile.getAbsolutePath}")
    System.exit(-1)
  }

  println(s"Using data_dir: ${dataDirFile.getAbsolutePath}")

  // Generate certificate if not provided as option or in the data dir
  if (!conf.tls.customCertificateLocation
      && !conf.tls.certificate.toFile.exists()) {
    println(s"No certificate found at path ${conf.tls.certificate}")
    println("Generating a X.509 certificate for the node")

    import coop.rchain.shared.Resources._
    // If there is a private key, use it for the certificate
    if (conf.tls.key.toFile.exists()) {
      println(s"Using secret key ${conf.tls.key}")
      Try(CertificateHelper.readKeyPair(conf.tls.key.toFile)) match {
        case Success(keyPair) =>
          withResource(new java.io.PrintWriter(conf.tls.certificate.toFile)) {
            _.write(CertificatePrinter.print(CertificateHelper.generate(keyPair)))
          }
        case Failure(e) =>
          println(s"Invalid secret key: ${e.getMessage}")
      }
    } else {
      println("Generating a PEM secret key for the node")
      val keyPair = CertificateHelper.generateKeyPair()
      withResource(new java.io.PrintWriter(conf.tls.certificate.toFile)) { pw =>
        pw.write(CertificatePrinter.print(CertificateHelper.generate(keyPair)))
      }
      withResource(new java.io.PrintWriter(conf.tls.key.toFile)) { pw =>
        pw.write(CertificatePrinter.printPrivateKey(keyPair.getPrivate))
      }
    }
  }

  if (!conf.tls.certificate.toFile.exists()) {
    println(s"Certificate file ${conf.tls.certificate} not found")
    System.exit(-1)
  }

  if (!conf.tls.key.toFile.exists()) {
    println(s"Secret key file ${conf.tls.certificate} not found")
    System.exit(-1)
  }

  private val name: String = {
    val publicKey = Try(CertificateHelper.fromFile(conf.tls.certificate.toFile)) match {
      case Success(c) => Some(c.getPublicKey)
      case Failure(e) =>
        println(s"Failed to read the X.509 certificate: ${e.getMessage}")
        System.exit(1)
        None
      case _ => None
    }

    val publicKeyHash = publicKey
      .flatMap(CertificateHelper.publicAddress)
      .map(Base16.encode)

    if (publicKeyHash.isEmpty) {
      println("Certificate must contain a secp256r1 EC Public Key")
      System.exit(1)
    }

    publicKeyHash.get
  }

  import ApplicativeError_._

  /** Configuration */
  private val port              = conf.server.port
  private val certificateFile   = conf.tls.certificate.toFile
  private val keyFile           = conf.tls.key.toFile
  private val address           = s"rnode://$name@$host:$port"
  private val src               = PeerNode.parse(address).right.get
  private val storagePath       = conf.server.dataDir.resolve("rspace")
  private val casperStoragePath = storagePath.resolve("casper")
  private val storageSize       = conf.server.mapSize
  private val defaultTimeout    = FiniteDuration(conf.server.defaultTimeout.toLong, MILLISECONDS)

  /** Final Effect + helper methods */
  type CommErrT[F[_], A] = EitherT[F, CommError, A]
  type Effect[A]         = CommErrT[Task, A]

  implicit class EitherEffectOps[A](e: Either[CommError, A]) {
    def toEffect: Effect[A] = EitherT[Task, CommError, A](e.pure[Task])
  }
  implicit class TaskEffectOps[A](t: Task[A]) {
    def toEffect: Effect[A] = t.liftM[CommErrT]
  }

  case class Servers(grpcServer: Server, metricsServer: MetricsServer, httpServer: HttpServer)

  def acquireServers(runtime: Runtime)(
      implicit
      log: Log[Task],
      nodeDiscovery: NodeDiscovery[Task],
      blockStore: BlockStore[Effect],
      oracle: SafetyOracle[Effect],
      casper: MultiParentCasper[Effect],
      nodeCoreMetrics: NodeMetrics[Task],
      jvmMetrics: JvmMetrics[Task]
  ): Effect[Servers] =
    for {
      grpcServer    <- GrpcServer.acquireServer[Effect](conf.grpcServer.port, runtime)
      metricsServer <- MetricsServer.create[Effect](conf.server.metricsPort)
      httpServer    <- HttpServer(conf.server.httpPort).pure[Effect]
    } yield Servers(grpcServer, metricsServer, httpServer)

  def startServers(servers: Servers)(
      implicit
      log: Log[Task],
  ): Effect[Unit] =
    for {
      _ <- servers.httpServer.start.toEffect
      _ <- servers.metricsServer.start.toEffect
      _ <- GrpcServer.start[Effect](servers.grpcServer)
    } yield ()

  def clearResources(servers: Servers, runtime: Runtime, casperRuntime: Runtime)(
      implicit
      time: Time[Task],
      transport: TransportLayer[Task],
      log: Log[Task],
      blockStore: BlockStore[Effect]): Unit =
    (for {
      _   <- log.info("Shutting down gRPC server...")
      _   <- Task.delay(servers.grpcServer.shutdown())
      _   <- log.info("Shutting down transport layer, broadcasting DISCONNECT")
      loc <- transport.local
      ts  <- time.currentMillis
      msg = ProtocolHelper.disconnect(loc)
      _   <- transport.shutdown(msg)
      _   <- log.info("Shutting down metrics server...")
      _   <- Task.delay(servers.metricsServer.stop())
      _   <- log.info("Shutting down HTTP server....")
      _   <- Task.delay(servers.httpServer.stop())
      _   <- log.info("Shutting down interpreter runtime ...")
      _   <- Task.delay(runtime.close)
      _   <- log.info("Shutting down Casper runtime ...")
      _   <- Task.delay(casperRuntime.close)
      _   <- log.info("Bringing BlockStore down ...")
      _   <- blockStore.close().value
      _   <- log.info("Goodbye.")
    } yield ()).unsafeRunSync

  def startReportJvmMetrics(implicit metrics: Metrics[Task],
                            jvmMetrics: JvmMetrics[Task]): Task[Unit] =
    Task.delay {
      import scala.concurrent.duration._
      scheduler.scheduleAtFixedRate(3.seconds, 3.second)(JvmMetrics.report[Task].unsafeRunSync)
    }

  def addShutdownHook(servers: Servers, runtime: Runtime, casperRuntime: Runtime)(
      implicit transport: TransportLayer[Task],
      log: Log[Task],
      time: Time[Task],
      blockStore: BlockStore[Effect]): Task[Unit] =
    Task.delay(sys.addShutdownHook(clearResources(servers, runtime, casperRuntime)))

  private def exit0: Task[Unit] = Task.delay(System.exit(0))

  private def nodeProgram(runtime: Runtime, casperRuntime: Runtime)(
      implicit
      log: Log[Task],
      time: Time[Task],
      timer: Timer[Effect],
      metrics: Metrics[Task],
      transport: TransportLayer[Task],
      nodeDiscovery: NodeDiscovery[Task],
      rpConnectons: ConnectionsCell[Task],
      blockStore: BlockStore[Effect],
      oracle: SafetyOracle[Effect],
      packetHandler: PacketHandler[Effect],
      lastApprovedBlock: LastApprovedBlock[Effect],
      casper: MultiParentCasper[Effect],
      nodeCoreMetrics: NodeMetrics[Task],
      jvmMetrics: JvmMetrics[Task]
  ): Effect[Unit] = {

    val handleCommunication = (pm: Protocol) =>
      NodeDiscovery[Effect].handleCommunications(pm) >>= {
        case NotHandled(_) => HandleMessages.handle[Effect](pm, defaultTimeout)
        case handled       => handled.pure[Effect]
    }
    for {
      _ <- Log[Effect].info(
            s"RChain Node ${BuildInfo.version} (${BuildInfo.gitHeadCommit.getOrElse("commit # unknown")})")
      servers <- acquireServers(runtime)
      _       <- addShutdownHook(servers, runtime, casperRuntime).toEffect
      _       <- startServers(servers)
      _       <- startReportJvmMetrics.toEffect
      _       <- TransportLayer[Effect].receive(handleCommunication)
      _       <- Log[Effect].info(s"Listening for traffic on $address.")
      res <- ApplicativeError_[Effect, CommError].attempt(
              if (conf.server.standalone) Log[Effect].info(s"Starting stand-alone node.")
              else
                Connect.connectToBootstrap[Effect](conf.server.bootstrap,
                                                   maxNumOfAttempts = 5,
                                                   defaultTimeout = defaultTimeout))
      _ <- EitherT.liftF(requestApprovedBlock[Effect].value.forkAndForget)
      _ <- if (res.isRight)
            MonadOps.forever((i: Int) =>
                               Connect
                                 .findAndConnect[Effect](defaultTimeout)
                                 .apply(i),
                             0)
          else ().pure[Effect]
      _ <- exit0.toEffect
    } yield ()

  }

  /**
    * Handles unrecoverable errors in program. Those are errors that should not happen in properly
    * configured enviornment and they mean immediate termination of the program
    */
  private def handleUnrecoverableErrors(prog: Effect[Unit])(implicit log: Log[Task]): Effect[Unit] =
    EitherT[Task, CommError, Unit](
      prog.value
        .onErrorHandleWith {
          case th if th.containsMessageWith("Error loading shared library libsodium.so") =>
            Log[Task]
              .error(
                "Libsodium is NOT installed on your system. Please install libsodium (https://github.com/jedisct1/libsodium) and try again.")
          case th =>
            th.getStackTrace.toList.traverse(ste => Log[Task].error(ste.toString))
        } *> exit0.as(Right(())))

  private def generateCasperPacketHandler(runtimeManager: RuntimeManager)(
      implicit
      log: Log[Task],
      time: Time[Task],
      timer: Timer[Effect],
      transport: TransportLayer[Task],
      lastApprovedBlock: LastApprovedBlock[Effect],
      nodeDiscovery: NodeDiscovery[Task],
      casper: MultiParentCasper[Effect],
      blockStore: BlockStore[Effect],
      oracle: SafetyOracle[Effect]): Effect[CasperPackageHandler[Effect]] =
    CasperPackageHandler
      .fromConfig[Effect, Effect](conf.casper, runtimeManager)

  private def generateCasperPacketHandler(implicit
                                          log: Log[Task],
                                          time: Time[Task],
                                          transport: TransportLayer[Task],
                                          nodeDiscovery: NodeDiscovery[Task],
                                          blockStore: BlockStore[Effect],
                                          lastApprovedBlock: LastApprovedBlock[Effect],
                                          casper: MultiParentCasper[Effect],
                                          casperConstructor: CasperPackageHandler[Effect])
    : PeerNode => PartialFunction[Packet, Effect[Option[Packet]]] =
    casperPacketHandler[Effect]

  private def timerEff(implicit timerTask: Timer[Task]): Timer[Effect] = new Timer[Effect] {
    override def clockRealTime(unit: TimeUnit): Effect[Long] =
      EitherT.liftF(timerTask.clockRealTime(unit))
    override def clockMonotonic(unit: TimeUnit): Effect[Long] =
      EitherT.liftF(timerTask.clockMonotonic(unit))
    override def sleep(duration: FiniteDuration): Effect[Unit] =
      EitherT.liftF(timerTask.sleep(duration))
    override def shift: Effect[Unit] = EitherT.liftF(timerTask.shift)
  }

  /**
    * Main node entry. Will Create instances of typeclasses and run the node program.
    */
  val main: Effect[Unit] = for {

    /** create typeclass instances */
    tcpConnections <- effects.tcpConnections.toEffect
    rpConnections  <- effects.rpConnections.toEffect
    log            = effects.log
    time           = effects.time
    timer          = timerEff
    sync = SyncInstances.syncEffect[CommError](commError => {
      new Exception(s"CommError: $commError")
    }, e => { UnknownCommError(e.getMessage) })
    metrics = diagnostics.metrics[Task]
    transport = effects.tcpTransportLayer(host, port, certificateFile, keyFile)(src)(scheduler,
                                                                                     tcpConnections,
                                                                                     log)
    kademliaRPC = effects.kademliaRPC(src, defaultTimeout)(metrics, transport)
    nodeDiscovery = effects.nodeDiscovery(src, defaultTimeout)(log,
                                                               time,
                                                               metrics,
                                                               transport,
                                                               kademliaRPC)
    blockStore = LMDBBlockStore.create[Effect](conf.blockstorage)(
      sync,
      Metrics.eitherT(Monad[Task], metrics))
    _                 <- blockStore.clear() // FIX-ME replace with a proper casper init when it's available
    oracle            = SafetyOracle.turanOracle[Effect](Applicative[Effect], blockStore)
    runtime           = Runtime.create(storagePath, storageSize)
    casperRuntime     = Runtime.create(casperStoragePath, storageSize)
    runtimeManager    = RuntimeManager.fromRuntime(casperRuntime)
    lastApprovedBlock <- LastApprovedBlock.of[Effect](implicitly, timer)

    casper <- {
      implicit val nodeDiscEff: NodeDiscovery[Effect] = NodeDiscovery
        .eitherTNodeDiscovery[CommError, Task](Monad[Task], nodeDiscovery)
      implicit val transportLayerEff: TransportLayer[Effect] =
        TransportLayer.eitherTTransportLayer[Task](Monad[Task], log, transport)
      implicit val logEff: Log[Effect]                             = Log.eitherTLog[CommError, Task](Monad[Task], log)
      implicit val timeEff: Time[Effect]                           = Time.eitherTTime[CommError, Task](Monad[Task], time)
      implicit val oracleEff: SafetyOracle[Effect]                 = oracle
      implicit val blockStoreEff: BlockStore[Effect]               = blockStore
      implicit val lastApprovedBlockEff: LastApprovedBlock[Effect] = lastApprovedBlock
      MultiParentCasperConstructor.of[Effect](runtimeManager, conf.casper)
    }

    casperPacketHandler <- generateCasperPacketHandler(runtimeManager)(log,
                                                                       time,
                                                                       timer,
                                                                       transport,
                                                                       lastApprovedBlock,
                                                                       nodeDiscovery,
                                                                       casper,
                                                                       blockStore,
                                                                       oracle)

    packetHandler = PacketHandler.pf[Effect](casperPacketHandler.handle)(
      Applicative[Effect],
      Log.eitherTLog(Monad[Task], log),
      ErrorHandler[Effect])
    nodeTransportLayerCoreMetrics = diagnostics.nodeCoreMetrics[Task]
    jvmMetrics                    = diagnostics.jvmMetrics[Task]

    /** run the node program */
    program = nodeProgram(runtime, casperRuntime)(
      log,
      time,
      timer,
      metrics,
      transport,
      nodeDiscovery,
      rpConnections,
      blockStore,
      oracle,
      packetHandler,
      lastApprovedBlock,
      casper,
      nodeCoreMetrics,
      jvmMetrics
    )
    _ <- handleUnrecoverableErrors(program)(log)
  } yield ()

}
