package coop.rchain.casper

import cats.Monad
import cats.data.OptionT
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, Sync, Timer}
import cats.implicits._
import com.google.protobuf.ByteString
import coop.rchain.blockstorage.BlockStore
import coop.rchain.casper.CasperConf.{Bootstrap, GenesisValidator, Standalone}
import coop.rchain.casper.genesis.Genesis
import coop.rchain.casper.protocol.{ApprovedBlock, ApprovedBlockRequest, BlockMessage, BlockRequest}
import coop.rchain.casper.util.comm.CommUtil.{
  blockPacketHandler,
  handleNewBlock,
  packetToApprovedBlockRequest,
  packetToBlockMessage,
  packetToBlockRequest
}
import coop.rchain.casper.util.comm.{ApproveBlockProtocol, BlockApproverProtocol, CommUtil}
import coop.rchain.casper.util.rholang.RuntimeManager
import coop.rchain.catscontrib._
import coop.rchain.comm.CommError.ErrorHandler
import coop.rchain.comm.{transport, PeerNode}
import coop.rchain.comm.discovery._
import coop.rchain.comm.protocol.rchain.Packet
import coop.rchain.comm.transport.CommMessages.packet
import coop.rchain.comm.transport._
import coop.rchain.shared.{Log, LogSource, Time}
import monix.execution.Scheduler

import scala.concurrent.duration._
import scala.util.Try

abstract class CasperPackageHandler[F[_]](implicit monad: Monad[F],
                                          multiParentCasper: MultiParentCasper[F],
                                          nodeDiscovery: NodeDiscovery[F],
                                          transportLayer: TransportLayer[F],
                                          log: Log[F],
                                          time: Time[F],
                                          handler: ErrorHandler[F],
                                          blockStore: BlockStore[F],
                                          lastApprovedBlock: LastApprovedBlock[F]) {
  private implicit val logSource: LogSource = LogSource(this.getClass)

  def handle(peer: PeerNode): PartialFunction[Packet, F[Option[Packet]]] =
    Function
      .unlift(
        (p: Packet) => {
          packetToBlockRequest(p) orElse packetToApprovedBlockRequest(p) orElse packetToBlockMessage(
            p)
        }
      )
      .andThen {
        case b @ (_: BlockMessage | _: BlockRequest) =>
          blockPacketHandler[F](peer, b)

        case _: ApprovedBlockRequest =>
          Log[F].info(s"CASPER: Received ApprovedBlockRequest from $peer") *>
            LastApprovedBlock[F].get.map(b =>
              Packet(transport.ApprovedBlock.id, b.toByteString).some)
      }

  private def blockPacketHandler[
      F[_]: Monad: MultiParentCasper: NodeDiscovery: TransportLayer: Log: Time: ErrorHandler: BlockStore](
      peer: PeerNode,
      msg: scalapb.GeneratedMessage): F[Option[Packet]] =
    msg match {
      case b: BlockMessage =>
        for {
          isOldBlock <- MultiParentCasper[F].contains(b)
          _ <- if (isOldBlock) {
                Log[F].info(
                  s"CASPER: Received block ${PrettyPrinter.buildString(b.blockHash)} again.")
              } else {
                handleNewBlock[F](b)
              }
        } yield none[Packet]

      case r: BlockRequest =>
        for {
          local      <- TransportLayer[F].local
          block      <- BlockStore[F].get(r.hash)
          serialized = block.map(_.toByteString)
          maybeMsg = serialized.map(serializedMessage =>
            packet(local, transport.BlockMessage, serializedMessage))
          send     <- maybeMsg.traverse(msg => TransportLayer[F].send(peer, msg))
          hash     = PrettyPrinter.buildString(r.hash)
          logIntro = s"CASPER: Received request for block $hash from $peer. "
          _ <- send match {
                case None    => Log[F].info(logIntro + "No response given since block not found.")
                case Some(_) => Log[F].info(logIntro + "Response sent.")
              }
        } yield none[Packet]
    }

  private def handleNewBlock[
      F[_]: Monad: MultiParentCasper: NodeDiscovery: TransportLayer: Log: Time: ErrorHandler](
      b: BlockMessage): F[Unit] =
    for {
      _ <- Log[F].info(s"CASPER: Received ${PrettyPrinter.buildString(b)}.")
      _ <- MultiParentCasper[F].addBlock(b)
    } yield ()

  private def packetToBlockMessage(msg: Packet): Option[BlockMessage] =
    if (msg.typeId == transport.BlockMessage.id)
      Try(BlockMessage.parseFrom(msg.content.toByteArray)).toOption
    else None

  private def packetToApprovedBlock(msg: Packet): Option[ApprovedBlock] =
    if (msg.typeId == transport.ApprovedBlock.id)
      Try(ApprovedBlock.parseFrom(msg.content.toByteArray)).toOption
    else None

  private def packetToApprovedBlockRequest(msg: Packet): Option[ApprovedBlockRequest] =
    if (msg.typeId == transport.ApprovedBlockRequest.id)
      Try(ApprovedBlockRequest.parseFrom(msg.content.toByteArray)).toOption
    else None

  private def packetToBlockRequest(msg: Packet): Option[BlockRequest] =
    if (msg.typeId == transport.BlockRequest.id)
      Try(BlockRequest.parseFrom(msg.content.toByteArray)).toOption
    else None
}

object CasperPackageHandler {
  def apply[F[_]](implicit instance: CasperPackageHandler[F]): CasperPackageHandler[F] =
    instance

  private implicit val logSource: LogSource = LogSource(this.getClass)

  // createGenesis == true
  class ApproveBlockCasper[F[_]] private (abp: ApproveBlockProtocol[F])(
      implicit monad: Monad[F],
      multiParentCasper: MultiParentCasper[F],
      nodeDiscovery: NodeDiscovery[F],
      transportLayer: TransportLayer[F],
      log: Log[F],
      time: Time[F],
      handler: ErrorHandler[F],
      blockStore: BlockStore[F],
      lastApprovedBlock: LastApprovedBlock[F])
      extends CasperPackageHandler[F] {
    private implicit val protocol: ApproveBlockProtocol[F] = abp

    override def handle(peer: PeerNode): PartialFunction[Packet, F[Option[Packet]]] =
      super.handle(peer) orElse ApproveBlockProtocol.blockApprovalPacketHandler[F](peer)
  }

  object ApproveBlockCasper {
    def apply[F[_]](abp: ApproveBlockProtocol[F])(
        implicit monad: Monad[F],
        multiParentCasper: MultiParentCasper[F],
        nodeDiscovery: NodeDiscovery[F],
        transportLayer: TransportLayer[F],
        log: Log[F],
        time: Time[F],
        handler: ErrorHandler[F],
        blockStore: BlockStore[F],
        lastApprovedBlock: LastApprovedBlock[F]): ApproveBlockCasper[F] =
      new ApproveBlockCasper[F](abp)
  }

  // approveGenesis == true
  class BlockApproverCasper[F[_]] private (val bap: BlockApproverProtocol[F])(
      implicit monad: Monad[F],
      multiParentCasper: MultiParentCasper[F],
      nodeDiscovery: NodeDiscovery[F],
      transportLayer: TransportLayer[F],
      log: Log[F],
      time: Time[F],
      handler: ErrorHandler[F],
      blockStore: BlockStore[F],
      lastApprovedBlock: LastApprovedBlock[F])
      extends CasperPackageHandler[F] {

    override def handle(peer: PeerNode): PartialFunction[Packet, F[Option[Packet]]] =
      super.handle(peer) orElse bap.unapprovedBlockPacketHandler(peer)

  }

  object BlockApproverCasper {
    def apply[F[_]](
        bap: BlockApproverProtocol[F]
    )(implicit monad: Monad[F],
      multiParentCasper: MultiParentCasper[F],
      nodeDiscovery: NodeDiscovery[F],
      transportLayer: TransportLayer[F],
      log: Log[F],
      time: Time[F],
      handler: ErrorHandler[F],
      blockStore: BlockStore[F],
      lastApprovedBlock: LastApprovedBlock[F]): BlockApproverCasper[F] =
      new BlockApproverCasper(bap)
  }

  // requestApprovedBlock
  class StandaloneCasper[F[_]] private (validators: Set[ByteString])(
      implicit monad: Monad[F],
      multiParentCasper: MultiParentCasper[F],
      nodeDiscovery: NodeDiscovery[F],
      transportLayer: TransportLayer[F],
      log: Log[F],
      time: Time[F],
      handler: ErrorHandler[F],
      blockStore: BlockStore[F],
      lastApprovedBlock: LastApprovedBlock[F])
      extends CasperPackageHandler[F] {

    private def packetToApprovedBlock(msg: Packet): Option[ApprovedBlock] =
      if (msg.typeId == transport.ApprovedBlock.id)
        Try(ApprovedBlock.parseFrom(msg.content.toByteArray)).toOption
      else None

    override def handle(peer: PeerNode): PartialFunction[Packet, F[Option[Packet]]] =
      super.handle(peer) orElse Function
        .unlift(
          (p: Packet) => packetToApprovedBlock(p)
        )
        .andThen { a: ApprovedBlock =>
          for {
            _       <- Log[F].info("CASPER: Received ApprovedBlock. Processing...")
            isValid <- Validate.approvedBlock[F](a.candidate, a.sigs, validators)
            _ <- if (isValid) {
                  for {
                    _       <- Log[F].info("CASPER: Valid ApprovedBlock received!")
                    genesis = a.candidate.get.block.get
                    _       <- LastApprovedBlock[F].complete(a)
                  } yield ()
                } else {
                  Log[F].info("CASPER: Invalid ApprovedBlock received; refusing to add.")
                }
          } yield none[Packet]
        }

  }

  object StandaloneCasper {
    def apply[F[_]](validators: Set[ByteString])(
        implicit monad: Monad[F],
        multiParentCasper: MultiParentCasper[F],
        nodeDiscovery: NodeDiscovery[F],
        transportLayer: TransportLayer[F],
        log: Log[F],
        time: Time[F],
        handler: ErrorHandler[F],
        blockStore: BlockStore[F],
        lastApprovedBlock: LastApprovedBlock[F]): StandaloneCasper[F] =
      new StandaloneCasper[F](validators)
  }

  def fromConfig[
      F[_]: Sync: Capture: NodeDiscovery: TransportLayer: Log: Time: Timer: ErrorHandler: SafetyOracle: BlockStore,
      G[_]: Concurrent: Capture: Log: Time: Timer: BlockStore: NodeDiscovery: ErrorHandler: TransportLayer: SafetyOracle: LastApprovedBlock: MultiParentCasper](
      conf: CasperConf,
      runtimeManager: RuntimeManager)(implicit scheduler: Scheduler): G[CasperPackageHandler[G]] =
    conf.mode match {
      case Bootstrap =>
        for {
          validators <- CasperConf.parseValidatorsFile[G](conf.knownValidatorsFile)
        } yield StandaloneCasper[G](validators)

      case Standalone(requiredSigs, duration, interval) =>
        for {
          genesis <- Genesis.fromInputFiles[G](conf.bondsFile,
                                               conf.numValidators,
                                               conf.genesisPath,
                                               conf.walletsFile,
                                               runtimeManager)
          validators <- CasperConf.parseValidatorsFile[G](conf.knownValidatorsFile)
          approvalProtocol <- ApproveBlockProtocol.create[G](block = genesis,
                                                             requiredSigs = requiredSigs,
                                                             duration = duration,
                                                             interval = interval,
                                                             validators = validators)
          _      <- Concurrent[G].start(approvalProtocol.run())
          casper = ApproveBlockCasper[G](approvalProtocol)
        } yield casper

      case GenesisValidator(requiredSigs) =>
        for {
          validatorId <- ValidatorIdentity.fromConfig[G](conf).map(_.get)
          genesis <- Genesis.fromInputFiles[G](conf.bondsFile,
                                               conf.numValidators,
                                               conf.genesisPath,
                                               conf.walletsFile,
                                               runtimeManager)
          blockApproverProtocol = new BlockApproverProtocol[G](validatorId, genesis, requiredSigs)
          casper                = BlockApproverCasper[G](blockApproverProtocol)
        } yield casper
    }

}

object MultiParentCasperConstructor {
  def of[F[_]: Sync: Capture: NodeDiscovery: TransportLayer: Log: Time: ErrorHandler: SafetyOracle: BlockStore: LastApprovedBlock](
      runtimeManager: RuntimeManager,
      casperConf: CasperConf)(implicit scheduler: Scheduler): F[MultiParentCasper[F]] =
    for {
      block        <- LastApprovedBlock[F].get
      validatorId  <- ValidatorIdentity.fromConfig[F](casperConf)
      blockMessage = block.candidate.flatMap(_.block).get
      blockHash    = block.candidate.flatMap(_.block.map(_.blockHash)).get
      _            <- BlockStore[F].put(blockHash, blockMessage)
      internalMap  <- BlockStore[F].asMap()
    } yield
      MultiParentCasper.hashSetCasper[F](
        runtimeManager,
        validatorId,
        blockMessage,
        internalMap
      )
}
