package coop.rchain.casper.helper

import cats.{Applicative, ApplicativeError, Id, Monad}
import cats.implicits._
import coop.rchain.casper.protocol._
import coop.rchain.casper.util.comm.CommUtil.casperPacketHandler
import coop.rchain.casper.util.comm.TransportLayerTestImpl
import coop.rchain.casper._
import coop.rchain.catscontrib._
import coop.rchain.comm._
import coop.rchain.crypto.signatures.Ed25519
import coop.rchain.metrics.Metrics
import coop.rchain.p2p.EffectsTestInstances._
import coop.rchain.p2p.effects.PacketHandler
import coop.rchain.comm.rp.{Connect, HandleMessages}
import HandleMessages.handle
import Connect._
import coop.rchain.comm.protocol.routing._
import coop.rchain.rholang.interpreter.Runtime
import java.nio.file.Files

import cats.effect.{Concurrent, Sync}
import cats.effect.concurrent.{Deferred, Ref}
import coop.rchain.casper.util.rholang.RuntimeManager
import monix.execution.Scheduler

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.collection.mutable
import coop.rchain.shared.PathOps.RichPath
import TaskContrib._
import scala.util.Random
import coop.rchain.catscontrib.effect.implicits._
import coop.rchain.comm.discovery.NodeDiscovery
import coop.rchain.comm.protocol.rchain.Packet
import coop.rchain.shared.{Cell, Log}
import monix.eval.Task

// Since we now share
class HashSetCasperTestNode(name: String,
                            val local: PeerNode,
                            tle: TransportLayerTestImpl[Task],
                            val genesis: BlockMessage,
                            sk: Array[Byte],
                            storageSize: Long = 1024L * 1024)(implicit scheduler: Scheduler) {

  import HashSetCasperTestNode.errorHandler

  private val storageDirectory = Files.createTempDirectory(s"hash-set-casper-test-$name")

  implicit val logEff            = new LogStub[Task]
  implicit val timeEff           = new LogicalTime[Task]
  implicit val nodeDiscoveryEff  = new NodeDiscoveryStub[Task]()
  implicit val transportLayerEff = tle
  implicit val metricEff         = new Metrics.MetricsNOP[Task]
  implicit val errorHandlerEff   = errorHandler[Task]
  val dir                        = BlockStoreTestFixture.dbDir
  implicit val blockStore        = BlockStoreTestFixture.create[Task](dir)
  // pre-population removed from internals of Casper
  blockStore.put(genesis.blockHash, genesis)
  implicit val turanOracleEffect = SafetyOracle.turanOracle[Task]
  implicit val connectionsCell   = Cell.const[Task, Connections](Connect.Connections.empty)

  val activeRuntime                  = Runtime.create(storageDirectory, storageSize)
  val runtimeManager                 = RuntimeManager.fromRuntime(activeRuntime)
  val defaultTimeout: FiniteDuration = FiniteDuration(1000, MILLISECONDS)

  val validatorId = ValidatorIdentity(Ed25519.toPublic(sk), sk, "ed25519")

  implicit val casperEff =
    blockStore
      .asMap()
      .map { m =>
        MultiParentCasper
          .hashSetCasper[Task](runtimeManager, Some(validatorId), genesis, m)
      }
      .unsafeRunSync

  implicit val constructor = new CasperPackageHandler[Task] {}

  private implicit val lastApprovedBlock: LastApprovedBlock[Task] =
    LastApprovedBlock.of[Task].unsafeRunSync
  lastApprovedBlock
    .complete(ApprovedBlock(Some(ApprovedBlockCandidate(Some(genesis)))))
    .unsafeRunSync

  implicit val packetHandlerEff =
    PacketHandler.pf[Task](peer => casperPacketHandler[Task](peer))

  def receive(): Unit = tle.receive(p => handle[Task](p, defaultTimeout))

  def tearDown(): Unit = {
    tearDownNode()
    dir.recursivelyDelete()
  }

  def tearDownNode(): Unit = {
    activeRuntime.close()
    blockStore.close()
  }
}

object HashSetCasperTestNode {
  def standalone(genesis: BlockMessage, sk: Array[Byte])(
      implicit scheduler: Scheduler): HashSetCasperTestNode = {
    val name     = "standalone"
    val identity = peerNode(name, 40400)
    val tle =
      new TransportLayerTestImpl[Task](identity, Map.empty[PeerNode, mutable.Queue[Protocol]])

    new HashSetCasperTestNode(name, identity, tle, genesis, sk)
  }

  def network(sks: IndexedSeq[Array[Byte]], genesis: BlockMessage)(
      implicit scheduler: Scheduler): IndexedSeq[HashSetCasperTestNode] = {
    val n         = sks.length
    val names     = (1 to n).map(i => s"node-$i")
    val peers     = names.map(peerNode(_, 40400))
    val msgQueues = peers.map(_ -> new mutable.Queue[Protocol]()).toMap

    val nodes =
      names.zip(peers).zip(sks).map {
        case ((n, p), sk) =>
          val tle = new TransportLayerTestImpl[Task](p, msgQueues)
          new HashSetCasperTestNode(n, p, tle, genesis, sk)
      }

    //make sure all nodes know about each other
    for {
      n <- nodes
      m <- nodes
      if n.local != m.local
    } {
      n.nodeDiscoveryEff.addNode(m.local)
    }

    nodes
  }

  def appErrF[F[_]: Sync] = new ApplicativeError[F, CommError] {
    def ap[A, B](ff: F[A => B])(fa: F[A]): F[B] = Applicative[F].ap[A, B](ff)(fa)
    def pure[A](x: A): F[A]                     = Applicative[F].pure[A](x)
    def raiseError[A](e: CommError): F[A] = {
      val errString = e match {
        case UnknownCommError(msg)                => s"UnknownCommError($msg)"
        case DatagramSizeError(size)              => s"DatagramSizeError($size)"
        case DatagramFramingError(ex)             => s"DatagramFramingError($ex)"
        case DatagramException(ex)                => s"DatagramException($ex)"
        case HeaderNotAvailable                   => "HeaderNotAvailable"
        case ProtocolException(th)                => s"ProtocolException($th)"
        case UnknownProtocolError(msg)            => s"UnknownProtocolError($msg)"
        case PublicKeyNotAvailable(node)          => s"PublicKeyNotAvailable($node)"
        case ParseError(msg)                      => s"ParseError($msg)"
        case EncryptionHandshakeIncorrectlySigned => "EncryptionHandshakeIncorrectlySigned"
        case BootstrapNotProvided                 => "BootstrapNotProvided"
        case PeerNodeNotFound(peer)               => s"PeerNodeNotFound($peer)"
        case PeerUnavailable(peer)                => s"PeerUnavailable($peer)"
        case MalformedMessage(pm)                 => s"MalformedMessage($pm)"
        case CouldNotConnectToBootstrap           => "CouldNotConnectToBootstrap"
        case InternalCommunicationError(msg)      => s"InternalCommunicationError($msg)"
        case TimeOut                              => "TimeOut"
        case _                                    => e.toString
      }

      Sync[F].delay(throw new Exception(errString))
    }

    def handleErrorWith[A](fa: F[A])(f: (CommError) => F[A]): F[A] = Sync[F].suspend {
      fa.flatMap {
        case commErr: CommError => f(commErr)
        case _                  => fa
      }
    }
  }

  def errorHandler[F[_]: Sync] = ApplicativeError_.applicativeError[F, CommError](appErrF[F])

  def randomBytes(length: Int): Array[Byte] = Array.fill(length)(Random.nextInt(256).toByte)

  def endpoint(port: Int): Endpoint = Endpoint("host", port, port)

  def peerNode(name: String, port: Int): PeerNode =
    PeerNode(NodeIdentifier(name.getBytes), endpoint(port))

}
