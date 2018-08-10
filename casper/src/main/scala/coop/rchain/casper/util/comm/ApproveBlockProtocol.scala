package coop.rchain.casper.util.comm

import cats.{FlatMap, Monad}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, Sync, Timer}
import cats.implicits._
import com.google.protobuf.ByteString
import coop.rchain.casper.protocol._
import coop.rchain.casper.{LastApprovedBlock, PrettyPrinter, Validate}
import coop.rchain.catscontrib.Capture
import coop.rchain.comm.discovery.NodeDiscovery
import coop.rchain.comm.protocol.rchain.Packet
import coop.rchain.comm.{transport, PeerNode}
import coop.rchain.comm.transport.TransportLayer
import coop.rchain.crypto.codec.Base16
import coop.rchain.crypto.hash.Blake2b256
import coop.rchain.shared._

import scala.concurrent.duration._
import scala.util.Try

/**
  * Bootstrap side of the protocol defined in
  * https://rchain.atlassian.net/wiki/spaces/CORE/pages/485556483/Initializing+the+Blockchain+--+Protocol+for+generating+the+Genesis+block
  */
class ApproveBlockProtocol[
    F[_]: Sync: Capture: NodeDiscovery: TransportLayer: Log: Time: Timer: LastApprovedBlock] private (
    val block: BlockMessage,
    val requiredSigs: Int,
    val validators: Set[ByteString],
    val start: Long,
    val duration: FiniteDuration,
    val interval: FiniteDuration,
    private val sigsF: Ref[F, Set[Signature]]) {
  private implicit val logSource: LogSource = LogSource(this.getClass)

  val candidate                         = ApprovedBlockCandidate(Some(block), requiredSigs)
  private val u                         = UnapprovedBlock(Some(candidate), start, duration.toMillis)
  private val serializedUnapprovedBlock = u.toByteString
  private val candidateHash             = PrettyPrinter.buildString(block.blockHash)
  private val sigData                   = Blake2b256.hash(candidate.toByteArray)

  def addApproval(a: BlockApproval): F[Unit] = {
    val validSig = for {
      c   <- a.candidate if c == this.candidate
      sig <- a.sig if Validate.signature(sigData, sig)
    } yield sig

    val isValid = for {
      validValidators <- Validate.approvedBlock[F](a.candidate, a.sig.toSeq, validators)
    } yield validValidators && validSig.isDefined

    val sender = a.sig.fold("<Empty Signature>")(sig => Base16.encode(sig.publicKey.toByteArray))

    FlatMap[F].ifM(isValid)(
      sigsF.update(_ + validSig.get) *> Log[F].info(
        s"APPROVAL: received block approval from $sender"),
      Log[F].warn(s"APPROVAL: ignoring invalid block approval from $sender")
    )
  }

  private def completeIf(time: Long, signatures: Set[Signature]): F[Unit] =
    if ((time >= start + duration.toMillis && signatures.size >= requiredSigs) || requiredSigs == 0) {
      for {
        _ <- LastApprovedBlock[F].complete(ApprovedBlock(Some(candidate), signatures.toSeq))
        _ <- sendApprovedBlock
      } yield ()
    } else Timer[F].sleep(interval) >> run()

  def run(): F[Unit] =
    for {
      _    <- sendUnapprovedBlock
      t    <- Timer[F].clockRealTime(MILLISECONDS)
      sigs <- sigsF.get
      _    <- completeIf(t, sigs)
    } yield Unit

  def currentSigs: F[Set[Signature]] = sigsF.get

  //TODO: potential optimization, only send to peers we have not
  //      received a valid signature from yet
  def sendUnapprovedBlock: F[Unit] =
    for {
      _ <- Log[F].info(s"APPROVAL: Beginning send of UnapprovedBlock $candidateHash to peers...")
      _ <- CommUtil.sendToPeers[F](transport.UnapprovedBlock, serializedUnapprovedBlock)
      _ <- Log[F].info(s"APPROVAL: Sent UnapprovedBlock $candidateHash to peers.")
    } yield ()

  def sendApprovedBlock: F[Unit] =
    for {
      a                       <- LastApprovedBlock[F].get
      serializedApprovedBlock = a.toByteString
      _                       <- Log[F].info(s"APPROVAL: Beginning send of ApprovedBlock $candidateHash to peers...")
      _                       <- CommUtil.sendToPeers[F](transport.ApprovedBlock, serializedApprovedBlock)
      _                       <- Log[F].info(s"APPROVAL: Sent ApprovedBlock $candidateHash to peers.")
    } yield Unit
}

object ApproveBlockProtocol {

  def apply[F[_]](implicit instance: ApproveBlockProtocol[F]): ApproveBlockProtocol[F] = instance

  def create[
      F[_]: Capture: NodeDiscovery: TransportLayer: Log: Time: Timer: Concurrent: LastApprovedBlock](
      block: BlockMessage,
      validators: Set[ByteString],
      requiredSigs: Int,
      duration: FiniteDuration,
      interval: FiniteDuration): F[ApproveBlockProtocol[F]] =
    for {
      now   <- Timer[F].clockRealTime(MILLISECONDS)
      sigsF <- Ref.of[F, Set[Signature]](Set.empty)
    } yield
      new ApproveBlockProtocol(block, requiredSigs, validators, now, duration, interval, sigsF)

  def blockApprovalPacketHandler[F[_]: Monad: ApproveBlockProtocol](
      peer: PeerNode): PartialFunction[Packet, F[Option[Packet]]] =
    Function
      .unlift(packetToBlockApproval)
      .andThen {
        case ba: BlockApproval =>
          for {
            _ <- ApproveBlockProtocol[F].addApproval(ba)
          } yield none[Packet]
      }

  private def packetToBlockApproval(msg: Packet): Option[BlockApproval] =
    if (msg.typeId == transport.BlockApproval.id)
      Try(BlockApproval.parseFrom(msg.content.toByteArray)).toOption
    else None

}
