package coop.rchain.casper.helper

import java.nio.ByteBuffer
import java.nio.file.{Files, Path}

import cats.Id
import cats.effect.Sync
import coop.rchain.blockstorage.{BlockStore, LMDBBlockStore}
import coop.rchain.casper.helper.BlockGenerator.{storeForStateWithChain, StateWithChain}
import coop.rchain.catscontrib.effect.implicits.syncId
import coop.rchain.shared.PathOps.RichPath
import org.lmdbjava.{Env, EnvFlags}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Suite}

trait BlockStoreFixture extends BeforeAndAfter { self: Suite =>
  def withStore[F[_]: Sync, R](f: BlockStore[F] => R): R = {
    val dir   = BlockStoreTestFixture.dbDir
    val store = BlockStoreTestFixture.create[F](dir)
    try {
      f(store)
    } finally {
      store.close()
      dir.recursivelyDelete()
    }
  }
}

object BlockStoreTestFixture {
  def env(path: Path,
          mapSize: Long,
          flags: List[EnvFlags] = List(EnvFlags.MDB_NOTLS)): Env[ByteBuffer] =
    Env
      .create()
      .setMapSize(mapSize)
      .setMaxDbs(8)
      .setMaxReaders(126)
      .open(path.toFile, flags: _*)

  def dbDir: Path   = Files.createTempDirectory("casper-block-store-test-")
  val mapSize: Long = 1024L * 1024L * 100L

  def create[F[_]: Sync](dir: Path): BlockStore[F] = {
    val environment = env(dir, mapSize)
    val blockStore  = LMDBBlockStore.createWith[F](environment, dir)
    blockStore
  }
}

trait BlockStoreTestFixture extends BeforeAndAfterAll { self: Suite =>

  val dir = BlockStoreTestFixture.dbDir

  def store[F[_]: Sync] = BlockStoreTestFixture.create[F](dir)

  implicit val blockStore = store[Id]

  implicit val blockStoreChain = storeForStateWithChain[StateWithChain](blockStore)

  override def afterAll(): Unit = {
    store.close()
    dir.recursivelyDelete()
  }
}
