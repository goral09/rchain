package coop.rchain.casper

import java.nio.file.Files

import cats.Comonad
import cats.data.EitherT
import cats.implicits._
import com.google.protobuf.ByteString
import coop.rchain.blockstorage.BlockStore
import coop.rchain.casper.genesis.Genesis
import coop.rchain.casper.genesis.contracts.ProofOfStakeValidator
import coop.rchain.casper.helper.{BlockStoreTestFixture, CasperEffect, HashSetCasperTestNode}
import coop.rchain.casper.protocol._
import coop.rchain.casper.util.ProtoUtil
import coop.rchain.casper.util.rholang.{InterpreterUtil, RuntimeManager}
import coop.rchain.catscontrib.TaskContrib.TaskOps
import coop.rchain.comm.transport
import coop.rchain.comm.transport.CommMessages.packet
import coop.rchain.crypto.hash.Blake2b256
import coop.rchain.crypto.signatures.Ed25519
import coop.rchain.models.PCost
import coop.rchain.rholang.interpreter.Runtime
import coop.rchain.shared.PathOps.RichPath
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable

class HashSetCasperTest extends FlatSpec with Matchers {
  import HashSetCasperTest._

  private val (otherSk, _)                = Ed25519.newKeyPair
  private val (validatorKeys, validators) = (1 to 4).map(_ => Ed25519.newKeyPair).unzip
  private val genesis                     = createGenesis(validators)

  //put a new casper instance at the start of each
  //test since we cannot reset it
  "HashSetCasper" should "accept deploys" in {
    val node = HashSetCasperTestNode.standalone(genesis, validatorKeys.head)
    import node._

    val deploy = ProtoUtil.basicDeploy(0)
    MultiParentCasper[Task].deploy(deploy).unsafeRunSync

    logEff.infos.size should be(1)
    logEff.infos.head.contains("CASPER: Received Deploy") should be(true)
    node.tearDown()
  }

  it should "not allow multiple threads to process the same block" in {
    val scheduler            = Scheduler.fixedPool("three-threads", 3)
    val (casperEff, cleanUp) = CasperEffect(validatorKeys.head, genesis)(scheduler)

    val deploy = ProtoUtil.basicDeploy(0)
    val testProgram = for {
      casper <- casperEff
      _      <- casper.deploy(deploy)
      block  <- casper.createBlock.map(_.get)
      result <- EitherT(
                 Task.racePair(casper.addBlock(block).value, casper.addBlock(block).value).flatMap {
                   case Left((statusA, running)) =>
                     running.join.map((statusA, _).tupled)

                   case Right((running, statusB)) =>
                     running.join.map((_, statusB).tupled)
                 })
    } yield result
    val threadStatuses: (BlockStatus, BlockStatus) =
      new TaskOps(testProgram.value)(scheduler).unsafeRunSync.right.get

    threadStatuses should matchPattern { case (Processing, Valid) | (Valid, Processing) => }
    cleanUp()
  }

  it should "create blocks based on deploys" in {
    val node            = HashSetCasperTestNode.standalone(genesis, validatorKeys.head)
    implicit val casper = node.casperEff

    val deploy = ProtoUtil.basicDeploy(0)
    MultiParentCasper[Task].deploy(deploy).unsafeRunSync

    val Some(block) = MultiParentCasper[Task].createBlock.unsafeRunSync
    val parents     = ProtoUtil.parents(block)
    val deploys     = block.body.get.newCode.flatMap(_.deploy)
    val storage     = blockTuplespaceContents(block)(taskComonad, casper)

    parents.size should be(1)
    parents.head should be(genesis.blockHash)
    deploys.size should be(1)
    deploys.head should be(deploy)
    storage.contains("@{0}!(0)") should be(true)
    node.tearDown()
  }

  it should "accept signed blocks" in {
    val node = HashSetCasperTestNode.standalone(genesis, validatorKeys.head)
    import node._

    val deploy = ProtoUtil.basicDeploy(0)
    MultiParentCasper[Task].deploy(deploy).unsafeRunSync

    val Some(signedBlock) = MultiParentCasper[Task].createBlock.unsafeRunSync

    MultiParentCasper[Task].addBlock(signedBlock).unsafeRunSync

    val logMessages = List(
      "CASPER: Received Deploy",
      "CASPER: Beginning send of Block #1",
      "CASPER: Sent",
      "CASPER: Added",
      "CASPER: New fork-choice tip is block"
    )

    logEff.warns.isEmpty should be(true)
    logEff.infos.zip(logMessages).forall { case (a, b) => a.startsWith(b) } should be(true)
    MultiParentCasper[Task].estimator.unsafeRunSync should be(IndexedSeq(signedBlock))
    node.tearDown()
  }

  it should "be able to create a chain of blocks from different deploys" in {
    val node = HashSetCasperTestNode.standalone(genesis, validatorKeys.head)
    import node._

    val deploys = Vector(
      "contract @\"add\"(@x, @y, ret) = { ret!(x + y) }",
      "new unforgable in { @\"add\"!(5, 7, *unforgable) }"
    ).map(s => ProtoUtil.termDeploy(InterpreterUtil.mkTerm(s).right.get))

    val Some(signedBlock1) = (MultiParentCasper[Task].deploy(deploys.head) *> MultiParentCasper[
      Task].createBlock).unsafeRunSync
    MultiParentCasper[Task].addBlock(signedBlock1)

    val Some(signedBlock2) =
      (MultiParentCasper[Task].deploy(deploys(1)) *> MultiParentCasper[Task].createBlock).unsafeRunSync
    MultiParentCasper[Task].addBlock(signedBlock2)
    val storage = blockTuplespaceContents(signedBlock2)(taskComonad, MultiParentCasper[Task])

    logEff.warns should be(Nil)
    ProtoUtil.parents(signedBlock2) should be(Seq(signedBlock1.blockHash))
    MultiParentCasper[Task].estimator should be(IndexedSeq(signedBlock2))
    storage.contains("!(12)") should be(true)
    node.tearDown()
  }

  it should "reject unsigned blocks" in {
    val node = HashSetCasperTestNode.standalone(genesis, validatorKeys.head)
    import node._

    val Some(block) = (MultiParentCasper[Task].deploy(ProtoUtil.basicDeploy(0)) *> MultiParentCasper[
      Task].createBlock).unsafeRunSync
    val invalidBlock = block.withSig(ByteString.EMPTY)

    MultiParentCasper[Task].addBlock(invalidBlock).unsafeRunSync

    logEff.warns.head.contains("CASPER: Ignoring block") should be(true)
    node.tearDownNode()
    validateBlockStore(node) { blockStore =>
      blockStore.get(block.blockHash).unsafeRunSync shouldBe None
    }
  }

  it should "reject blocks not from bonded validators" in {
    val node = HashSetCasperTestNode.standalone(genesis, otherSk)
    import node._

    val Some(signedBlock) =
      (MultiParentCasper[Task].deploy(ProtoUtil.basicDeploy(0)) *> MultiParentCasper[Task].createBlock)
        .unsafeRunSync

    (MultiParentCasper[Task].addBlock(signedBlock)).unsafeRunSync

    logEff.warns.head.contains("CASPER: Ignoring block") should be(true)
    node.tearDownNode()
    validateBlockStore(node) { blockStore =>
      blockStore.get(signedBlock.blockHash) shouldBe None
    }
  }

  it should "propose blocks it adds to peers" in {
    val nodes  = HashSetCasperTestNode.network(validatorKeys.take(2), genesis)
    val deploy = ProtoUtil.basicDeploy(0)

    val Some(signedBlock) = (nodes(0).casperEff
      .deploy(deploy) *> nodes(0).casperEff.createBlock).unsafeRunSync

    nodes(0).casperEff.addBlock(signedBlock).unsafeRunSync
    nodes(1).receive()

    val received = nodes(1).casperEff.contains(signedBlock).unsafeRunSync

    received should be(true)

    nodes.foreach(_.tearDownNode())
    nodes.foreach { node =>
      validateBlockStore(node) { blockStore =>
        blockStore.get(signedBlock.blockHash) shouldBe Some(signedBlock)
      }
    }
  }

  it should "add a valid block from peer" in {
    val nodes  = HashSetCasperTestNode.network(validatorKeys.take(2), genesis)
    val deploy = ProtoUtil.basicDeploy(1)

    val Some(signedBlock1Prime) = (nodes(0).casperEff
      .deploy(deploy) *> nodes(0).casperEff.createBlock).unsafeRunSync

    nodes(0).casperEff.addBlock(signedBlock1Prime).unsafeRunSync
    nodes(1).receive()

    nodes(1).logEff.infos.count(_ startsWith "CASPER: Added") should be(1)
    nodes(1).logEff.warns.count(_ startsWith "CASPER: Recording invalid block") should be(0)

    nodes.foreach(_.tearDownNode())
    nodes.foreach { node =>
      validateBlockStore(node) { blockStore =>
        blockStore.get(signedBlock1Prime.blockHash).unsafeRunSync shouldBe Some(signedBlock1Prime)
      }
    }
  }

  it should "reject addBlock when there exist deploy by the same (user, millisecond timestamp) in the chain" in {
    val nodes = HashSetCasperTestNode.network(validatorKeys.take(2), genesis)

    val deploys = (0 to 2).map(i => ProtoUtil.basicDeploy(i))
    val deployPrim0 = deploys(1).withRaw(
      deploys(1).getRaw.withTimestamp(deploys(0).getRaw.timestamp).withUser(deploys(0).getRaw.user)
    ) // deployPrim0 has the same (user, millisecond timestamp) with deploys(0)

    val Some(signedBlock1) =
      (nodes(0).casperEff.deploy(deploys(0)) *> nodes(0).casperEff.createBlock).unsafeRunSync
    nodes(0).casperEff.addBlock(signedBlock1).unsafeRunSync
    nodes(1).receive() // receive block1

    val Some(signedBlock2) =
      (nodes(0).casperEff.deploy(deploys(1)) *> nodes(0).casperEff.createBlock).unsafeRunSync
    nodes(0).casperEff.addBlock(signedBlock2).unsafeRunSync
    nodes(1).receive() // receive block2

    val Some(signedBlock3) =
      (nodes(0).casperEff.deploy(deploys(2)) *> nodes(0).casperEff.createBlock).unsafeRunSync
    nodes(0).casperEff.addBlock(signedBlock3).unsafeRunSync
    nodes(1).receive() // receive block3

    val Some(signedBlock4) = (nodes(1).casperEff
      .deploy(deployPrim0) *> nodes(1).casperEff.createBlock).unsafeRunSync
    nodes(1).casperEff.addBlock(signedBlock4).unsafeRunSync // should fail
    nodes(0).receive()

    nodes(1).casperEff.contains(signedBlock3).unsafeRunSync should be(true)
    nodes(1).casperEff.contains(signedBlock4).unsafeRunSync should be(false)
    nodes(0).casperEff.contains(signedBlock4).unsafeRunSync should be(false)

    nodes(1).logEff.warns
      .count(_ contains "found deploy by the same (user, millisecond timestamp) produced") should be(
      1)
    nodes.foreach(_.tearDownNode())

    nodes.foreach { node =>
      validateBlockStore(node) { blockStore =>
        blockStore.get(signedBlock1.blockHash).unsafeRunSync shouldBe Some(signedBlock1)
        blockStore.get(signedBlock2.blockHash).unsafeRunSync shouldBe Some(signedBlock2)
        blockStore.get(signedBlock3.blockHash).unsafeRunSync shouldBe Some(signedBlock3)
      }
    }
  }

  it should "ask peers for blocks it is missing" in {
    val nodes = HashSetCasperTestNode.network(validatorKeys.take(3), genesis)
    val deploys = Vector(
      "for(_ <- @1){ Nil } | @1!(1)",
      "@2!(2)"
    ).map(s => ProtoUtil.termDeploy(InterpreterUtil.mkTerm(s).right.get))

    val Some(signedBlock1) =
      (nodes(0).casperEff.deploy(deploys(0)) *> nodes(0).casperEff.createBlock).unsafeRunSync

    nodes(0).casperEff.addBlock(signedBlock1).unsafeRunSync
    nodes(1).receive()
    nodes(2).transportLayerEff.msgQueues(nodes(2).local).clear //nodes(2) misses this block

    val Some(signedBlock2) =
      (nodes(0).casperEff.deploy(deploys(1)) *> nodes(0).casperEff.createBlock).unsafeRunSync

    nodes(0).casperEff.addBlock(signedBlock2).unsafeRunSync
    nodes(1).receive() //receives block2
    nodes(2).receive() //receives block2; asks for block1
    nodes(1).receive() //receives request for block1; sends block1
    nodes(2).receive() //receives block1; adds both block1 and block2

    nodes(2).casperEff.contains(signedBlock1).unsafeRunSync should be(true)
    nodes(2).casperEff.contains(signedBlock2).unsafeRunSync should be(true)

    nodes(2).logEff.infos
      .count(_ startsWith "CASPER: Beginning request of missing block") should be(1)
    nodes(1).logEff.infos.count(s =>
      (s startsWith "CASPER: Received request for block") && (s endsWith "Response sent.")) should be(
      1)

    nodes.foreach(_.tearDownNode())
    nodes.foreach { node =>
      validateBlockStore(node) { blockStore =>
        blockStore.get(signedBlock1.blockHash).unsafeRunSync shouldBe Some(signedBlock1)
        blockStore.get(signedBlock2.blockHash).unsafeRunSync shouldBe Some(signedBlock2)
      }
    }
  }

  it should "ignore adding equivocation blocks" in {
    val node = HashSetCasperTestNode.standalone(genesis, validatorKeys.head)

    // Creates a pair that constitutes equivocation blocks
    val Some(signedBlock1) =
      (node.casperEff.deploy(ProtoUtil.basicDeploy(0)) *> node.casperEff.createBlock).unsafeRunSync
    val Some(signedBlock1Prime) =
      (node.casperEff.deploy(ProtoUtil.basicDeploy(1)) *> node.casperEff.createBlock).unsafeRunSync

    node.casperEff.addBlock(signedBlock1).unsafeRunSync
    node.casperEff.addBlock(signedBlock1Prime).unsafeRunSync

    node.casperEff.contains(signedBlock1).unsafeRunSync should be(true)
    node.casperEff
      .contains(signedBlock1Prime)
      .unsafeRunSync should be(false) // Ignores addition of equivocation pair

    node.tearDownNode()
    validateBlockStore(node) { blockStore =>
      blockStore.get(signedBlock1.blockHash).unsafeRunSync shouldBe Some(signedBlock1)
      blockStore.get(signedBlock1Prime.blockHash).unsafeRunSync shouldBe None
    }
  }

  // See [[/docs/casper/images/minimal_equivocation_neglect.png]] but cross out genesis block
  it should "not ignore equivocation blocks that are required for parents of proper nodes" in {
    val nodes   = HashSetCasperTestNode.network(validatorKeys.take(3), genesis)
    val deploys = (0 to 5).map(i => ProtoUtil.basicDeploy(i))

    // Creates a pair that constitutes equivocation blocks
    val Some(signedBlock1) =
      (nodes(0).casperEff.deploy(deploys(0)) *> nodes(0).casperEff.createBlock).unsafeRunSync
    val Some(signedBlock1Prime) = (nodes(0).casperEff
      .deploy(deploys(1)) *> nodes(0).casperEff.createBlock).unsafeRunSync

    nodes(1).casperEff.addBlock(signedBlock1).unsafeRunSync
    nodes(0).transportLayerEff.msgQueues(nodes(0).local).clear //nodes(0) misses this block
    nodes(2).transportLayerEff.msgQueues(nodes(2).local).clear //nodes(2) misses this block

    nodes(0).casperEff.addBlock(signedBlock1Prime).unsafeRunSync
    nodes(1).transportLayerEff.msgQueues(nodes(1).local).clear //nodes(1) misses this block
    nodes(2).receive()

    nodes(1).casperEff.contains(signedBlock1).unsafeRunSync should be(true)
    nodes(2).casperEff.contains(signedBlock1).unsafeRunSync should be(false)
    nodes(1).casperEff.contains(signedBlock1Prime).unsafeRunSync should be(false)
    nodes(2).casperEff.contains(signedBlock1Prime).unsafeRunSync should be(true)

    val Some(signedBlock2) =
      (nodes(1).casperEff.deploy(deploys(2)) *> nodes(1).casperEff.createBlock).unsafeRunSync
    val Some(signedBlock3) =
      (nodes(2).casperEff.deploy(deploys(3)) *> nodes(2).casperEff.createBlock).unsafeRunSync

    nodes(2).casperEff.addBlock(signedBlock3).unsafeRunSync
    nodes(1).casperEff.addBlock(signedBlock2).unsafeRunSync
    nodes(2).transportLayerEff.msgQueues(nodes(2).local).clear //nodes(2) ignores block2
    nodes(1).receive() // receives block3; asks for block1'
    nodes(2).receive() // receives request for block1'; sends block1'
    nodes(1).receive() // receives block1'; adds both block3 and block1'

    nodes(1).casperEff.contains(signedBlock3).unsafeRunSync should be(true)
    nodes(1).casperEff.contains(signedBlock1Prime).unsafeRunSync should be(true)

    val Some(signedBlock4) =
      (nodes(1).casperEff.deploy(deploys(4)) *> nodes(1).casperEff.createBlock).unsafeRunSync

    nodes(1).casperEff.addBlock(signedBlock4).unsafeRunSync

    // Node 1 should contain both blocks constituting the equivocation
    nodes(1).casperEff.contains(signedBlock1).unsafeRunSync should be(true)
    nodes(1).casperEff.contains(signedBlock1Prime).unsafeRunSync should be(true)

    nodes(1).casperEff
      .contains(signedBlock4)
      .unsafeRunSync should be(true) // However, in invalidBlockTracker

    nodes(1).logEff.infos.count(_ startsWith "CASPER: Added admissible equivocation") should be(1)
    nodes(1).logEff.warns.count(_ startsWith "CASPER: Recording invalid block") should be(1)

    nodes(1).casperEff.normalizedInitialFault(ProtoUtil.weightMap(genesis)) should be(
      1f / (1f + 3f + 5f + 7f))
    nodes.foreach(_.tearDownNode())

    validateBlockStore(nodes(0)) { blockStore =>
      blockStore.get(signedBlock1.blockHash).unsafeRunSync shouldBe None
      blockStore.get(signedBlock1Prime.blockHash).unsafeRunSync shouldBe Some(signedBlock1Prime)
    }
    validateBlockStore(nodes(1)) { blockStore =>
      blockStore.get(signedBlock2.blockHash).unsafeRunSync shouldBe Some(signedBlock2)
      blockStore.get(signedBlock4.blockHash).unsafeRunSync shouldBe Some(signedBlock4)
    }
    validateBlockStore(nodes(2)) { blockStore =>
      blockStore.get(signedBlock3.blockHash).unsafeRunSync shouldBe Some(signedBlock3)
      blockStore.get(signedBlock1Prime.blockHash).unsafeRunSync shouldBe Some(signedBlock1Prime)
    }
  }

  it should "prepare to slash an block that includes a invalid block pointer" in {
    val nodes           = HashSetCasperTestNode.network(validatorKeys.take(3), genesis)
    val deploys         = (0 to 5).map(i => ProtoUtil.basicDeploy(i))
    val deploysWithCost = deploys.map(d => DeployCost().withDeploy(d).withCost(PCost(10L, 1)))

    val Some(signedBlock) =
      (nodes(0).casperEff.deploy(deploys(0)) *> nodes(0).casperEff.createBlock).unsafeRunSync
    val signedInvalidBlock = signedBlock.withSeqNum(-2) // Invalid seq num

    val blockWithInvalidJustification =
      buildBlockWithInvalidJustification(nodes, deploysWithCost, signedInvalidBlock)

    nodes(1).casperEff.addBlock(blockWithInvalidJustification)
    nodes(0).transportLayerEff
      .msgQueues(nodes(0).local)
      .clear // nodes(0) rejects normal adding process for blockThatPointsToInvalidBlock
    val signedInvalidBlockPacketMessage =
      packet(nodes(1).local, transport.BlockMessage, signedInvalidBlock.toByteString)
    nodes(0).transportLayerEff.send(nodes(1).local, signedInvalidBlockPacketMessage)
    nodes(1).receive() // receives signedInvalidBlock; attempts to add both blocks

    nodes(1).logEff.warns.count(_ startsWith "CASPER: Recording invalid block") should be(2)
    nodes.foreach(_.tearDown())
  }

  it should "handle a long chain of block requests appropriately" in {
    val nodes = HashSetCasperTestNode.network(validatorKeys.take(2), genesis)

    (0 to 9).foreach { i =>
      val deploy = ProtoUtil.basicDeploy(i)
      val Some(block) =
        (nodes(0).casperEff.deploy(deploy) *> nodes(0).casperEff.createBlock).unsafeRunSync

      nodes(0).casperEff.addBlock(block).unsafeRunSync
      nodes(1).transportLayerEff.msgQueues(nodes(1).local).clear //nodes(1) misses this block
    }
    val Some(block) = (nodes(0).casperEff
      .deploy(ProtoUtil.basicDeploy(10)) *> nodes(0).casperEff.createBlock).unsafeRunSync
    nodes(0).casperEff.addBlock(block).unsafeRunSync

    (0 to 10).foreach { i =>
      nodes(1).receive()
      nodes(0).receive()
    }

    nodes(1).logEff.infos
      .count(_ startsWith "CASPER: Beginning request of missing block") should be(10)
    nodes(0).logEff.infos.count(s =>
      (s startsWith "CASPER: Received request for block") && (s endsWith "Response sent.")) should be(
      10)

    nodes.foreach(_.tearDown())
  }

  it should "increment last finalized block as appropriate in round robin" in {
    val stake                 = 10
    val equalBonds            = validators.zipWithIndex.map { case (v, _) => v -> stake }.toMap
    val genesisWithEqualBonds = buildGenesis(equalBonds)
    val nodes                 = HashSetCasperTestNode.network(validatorKeys.take(3), genesisWithEqualBonds)
    val deploys               = (0 to 7).map(i => ProtoUtil.basicDeploy(i))

    val Some(block1) =
      (nodes(0).casperEff.deploy(deploys(0)) *> nodes(0).casperEff.createBlock).unsafeRunSync
    nodes(0).casperEff.addBlock(block1).unsafeRunSync
    nodes(1).receive()
    nodes(2).receive()

    val Some(block2) =
      (nodes(1).casperEff.deploy(deploys(1)) *> nodes(1).casperEff.createBlock).unsafeRunSync
    nodes(1).casperEff.addBlock(block2).unsafeRunSync
    nodes(0).receive()
    nodes(2).receive()

    val Some(block3) =
      (nodes(2).casperEff.deploy(deploys(2)) *> nodes(2).casperEff.createBlock).unsafeRunSync
    nodes(2).casperEff.addBlock(block3).unsafeRunSync
    nodes(0).receive()
    nodes(1).receive()

    val Some(block4) =
      (nodes(0).casperEff.deploy(deploys(3)) *> nodes(0).casperEff.createBlock).unsafeRunSync
    nodes(0).casperEff.addBlock(block4).unsafeRunSync
    nodes(1).receive()
    nodes(2).receive()

    val Some(block5) =
      (nodes(1).casperEff.deploy(deploys(4)) *> nodes(1).casperEff.createBlock).unsafeRunSync
    nodes(1).casperEff.addBlock(block5).unsafeRunSync
    nodes(0).receive()
    nodes(2).receive()

    nodes(0).casperEff.lastFinalizedBlock should be(genesisWithEqualBonds)

    val Some(block6) =
      (nodes(2).casperEff.deploy(deploys(5)) *> nodes(2).casperEff.createBlock).unsafeRunSync
    nodes(2).casperEff.addBlock(block6).unsafeRunSync
    nodes(0).receive()
    nodes(1).receive()

    nodes(0).casperEff.lastFinalizedBlock should be(block1)

    val Some(block7) =
      (nodes(0).casperEff.deploy(deploys(6)) *> nodes(0).casperEff.createBlock).unsafeRunSync
    nodes(0).casperEff.addBlock(block7).unsafeRunSync
    nodes(1).receive()
    nodes(2).receive()

    nodes(0).casperEff.lastFinalizedBlock should be(block2)

    val Some(block8) =
      (nodes(1).casperEff.deploy(deploys(7)) *> nodes(1).casperEff.createBlock).unsafeRunSync
    nodes(1).casperEff.addBlock(block8).unsafeRunSync
    nodes(0).receive()
    nodes(2).receive()

    nodes(0).casperEff.lastFinalizedBlock should be(block3)

    nodes.foreach(_.tearDown())
  }

  private def buildBlockWithInvalidJustification(nodes: IndexedSeq[HashSetCasperTestNode],
                                                 deploys: immutable.IndexedSeq[DeployCost],
                                                 signedInvalidBlock: BlockMessage) = {
    val postState     = RChainState().withBonds(ProtoUtil.bonds(genesis)).withBlockNumber(2)
    val postStateHash = Blake2b256.hash(postState.toByteArray)
    val header = Header()
      .withPostStateHash(ByteString.copyFrom(postStateHash))
      .withParentsHashList(Seq(signedInvalidBlock.blockHash))
    val blockHash = Blake2b256.hash(header.toByteArray)
    val body      = Body().withPostState(postState).withNewCode(deploys)
    val serializedJustifications =
      Seq(Justification(signedInvalidBlock.sender, signedInvalidBlock.blockHash))
    val serializedBlockHash = ByteString.copyFrom(blockHash)
    val blockThatPointsToInvalidBlock =
      BlockMessage(serializedBlockHash, Some(header), Some(body), serializedJustifications)
    ProtoUtil.signBlock(blockThatPointsToInvalidBlock,
                        nodes(1).casperEff.blockDag.unsafeRunSync,
                        validators(1),
                        validatorKeys(1),
                        "ed25519",
                        Ed25519.sign _)
  }
}

object HashSetCasperTest {
  implicit val taskComonad = new Comonad[Task] {
    override def extract[A](x: Task[A]): A                              = x.unsafeRunSync
    override def coflatMap[A, B](fa: Task[A])(f: Task[A] => B): Task[B] = Task.delay(f(fa))
    override def map[A, B](fa: Task[A])(f: A => B): Task[B]             = fa.map(f)
  }

  def validateBlockStore[R](node: HashSetCasperTestNode)(f: BlockStore[Task] => R) = {
    val bs = BlockStoreTestFixture.create[Task](node.dir)
    f(bs)
    bs.close()
    node.dir.recursivelyDelete()
  }

  def blockTuplespaceContents[F[_]: Comonad](block: BlockMessage)(
      implicit casper: MultiParentCasper[F]): String = {
    val tsHash = block.body.get.postState.get.tuplespace
    MultiParentCasper[F].storageContents(tsHash).extract
  }

  def createGenesis(validators: Seq[Array[Byte]]): BlockMessage = {
    val bonds = validators.zipWithIndex.map { case (v, i) => v -> (2 * i + 1) }.toMap
    buildGenesis(bonds)
  }

  def buildGenesis(bonds: Map[Array[Byte], Int]): BlockMessage = {
    val initial           = Genesis.withoutContracts(bonds = bonds, version = 0L, timestamp = 0L)
    val storageDirectory  = Files.createTempDirectory(s"hash-set-casper-test-genesis")
    val storageSize: Long = 1024L * 1024
    val activeRuntime     = Runtime.create(storageDirectory, storageSize)
    val runtimeManager    = RuntimeManager.fromRuntime(activeRuntime)
    val emptyStateHash    = runtimeManager.emptyStateHash
    val genesis = Genesis.withContracts(
      initial,
      bonds.map(bond => ProofOfStakeValidator(bond._1, bond._2)).toSeq,
      Nil,
      emptyStateHash,
      runtimeManager)
    activeRuntime.close()
    genesis
  }
}
