package coop.rchain.rholang.interpreter

import cats.effect.{Async, Concurrent, Sync, Timer}
import cats.effect.concurrent.{MVar, Ref, Semaphore}
import org.scalatest.FlatSpec
import cats.implicits._
import monix.eval.Task
import monix.execution.ExecutionModel
import monix.execution.atomic.AtomicInt
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.{Await, SyncVar}
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

class TransactionTest extends FlatSpec {

  behavior of "Reducer in the face of sync-ing over cost accounting"

  val monotonicClock = AtomicInt(1)

  trait Bank[F[_]] {
    def deposit(money: Int): F[Unit]
    def withdraw(money: Int): F[Int]
    def getAccountState: F[Int]
  }

  object Bank {
    def apply[F[_]](implicit ev: Bank[F]): Bank[F] = ev

    def ofMVar[F[_]: Concurrent](initState: Int): F[Bank[F]] = MVar.of[F, Int](initState).map {
      initState =>
        new Bank[F] {
          override def deposit(money: Int): F[Unit] =
            for {
              curr <- initState.take
              _    <- initState.put(curr + money)
            } yield ()
          override def withdraw(money: Int): F[Int] =
            for {
              _    <- Sync[F].unit
              curr <- initState.take
              _ <- if (curr >= money)
                    initState.put(curr - money)
                  else
                    Sync[F]
                      .raiseError(new RuntimeException(
                        s"[${monotonicClock.getAndIncrement()}]: Only $curr left which is less than requested $money."))
            } yield money
          override def getAccountState: F[Int] = initState.read
        }
    }

    // Doesn't work ATM because `Semaphore` uses bracketCase internally which is not defined for the
    // monix Task we use.
    def of[F[_]: Sync](initState: Int, semaphore: Semaphore[F]): F[Bank[F]] =
      Ref.of[F, Int](initState).map { initState =>
        new Bank[F] {
          override def deposit(money: Int): F[Unit] =
            initState.update(_ + money)
          override def withdraw(money: Int): F[Int] =
            for {
              _    <- Sync[F].unit
              _    = semaphore.acquire
              curr <- initState.get
              _ <- if (curr >= money)
                    initState
                      .update(_ - money)
                      .map(_ => curr - money)
                      .map(m => {
                        semaphore.release;
                        m
                      })
                  else {
                    semaphore.release
                    Sync[F]
                      .raiseError(new RuntimeException(
                        s"[${monotonicClock.getAndIncrement()}]: Only $curr left which is more than requested $money."))
                  }
            } yield money
          override def getAccountState: F[Int] = initState.get
        }
      }
  }

  it should "work" in {
    def withdraw[F[_]: Bank: Sync](id: String, amount: Int): F[Int] =
      Bank[F].withdraw(amount) <*
        Sync[F].delay(println(s"[${monotonicClock.getAndIncrement()}]: $id withdrew $amount."))

    def deposit[F[_]: Bank: Sync](id: String, amount: Int): F[Unit] =
      Bank[F].deposit(amount) <*
        Sync[F].delay(println(s"[${monotonicClock.getAndIncrement()}]: $id deposited $amount."))

    def play[F[_]: Timer: Async](id: String, duration: FiniteDuration): F[Unit] =
      Sync[F].delay(println(s"[${monotonicClock.getAndIncrement()}]: $id will play for $duration.")) *>
        Timer[F].sleep(duration)

    def balance[F[_]: Bank]: F[Int] =
      Bank[F].getAccountState

    val idA = "PersonA"
    val idB = "PersonB"

    def personA(implicit bank: Bank[Task], timer: Timer[Task], async: Async[Task]): Task[Int] =
      for {
        _            <- deposit(idA, 100)
        _            <- withdraw(idA, 30)
        _            <- withdraw(idA, 20)
        _            <- withdraw(idA, 10)
        _            <- withdraw(idA, 50)
        finalBalance <- balance
      } yield finalBalance

    def personB(implicit bank: Bank[Task], timer: Timer[Task], async: Async[Task]): Task[Int] =
      for {
        _            <- deposit(idB, 10)
        _            <- withdraw(idB, 40)
        _            <- withdraw(idB, 10)
        _            <- withdraw(idB, 60)
        finalBalance <- balance
      } yield finalBalance

    val program: Task[Either[Throwable, List[Int]]] = for {
      bank <- Bank.ofMVar[Task](0)
      tasks = {
        implicit val b: Bank[Task] = bank
        List(personA, personB)
      }
      bs <- Task.gatherUnordered(tasks).attempt
    } yield bs

    val res = Await.result(program.executeWithModel(ExecutionModel.AlwaysAsyncExecution).runAsync,
                           10.seconds)
    println(s"Result $res")
  }

}
