package coop.rchain.rholang.interpreter.accounting

import com.google.protobuf.ByteString
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}

trait Costs {

  type Cost = BigInt

  final val BOOLEAN_COST: Cost = 1
  final val INT_COST: Cost     = 2

  def stringCost(str: String): Cost               = str.getBytes().size
  def byteArrayCost(byteString: ByteString): Cost = BigInt(byteString.size())
  def collectionCost(l: List[_]): Cost            = BigInt(l.size)

  final val SUM_COST: Cost            = 3
  final val SUBTRACTION_COST: Cost    = 3
  final val COMPARISON_COST: Cost     = 3
  final val MULTIPLICATION_COST: Cost = 9
  final val DIVISION_COST: Cost       = 9

  final val UNBUNDLE_RECEIVE_COST = 10

  final val METHOD_CALL_COST  = 10
  final val VAR_EVAL_COST     = 10
  final val SEND_EVAL_COST    = 11
  final val RECEIVE_EVAL_COST = 11
  final val CHANNEL_EVAL_COST = 11
  final val NEW_EVAL_COST     = 10
  final val MATCH_EVAL_COST   = 12

  implicit def toStorageCostOps[A <: GeneratedMessage with Message[A]](a: Seq[A])(
      implicit gm: GeneratedMessageCompanion[A]) = new StorageCostOps(a: _*)(gm)

  implicit def toStorageCostOps[A <: GeneratedMessage with Message[A]](a: A)(
      implicit gm: GeneratedMessageCompanion[A]) = new StorageCostOps(a)(gm)

  class StorageCostOps[A <: GeneratedMessage with Message[A]](a: A*)(
      gm: GeneratedMessageCompanion[A]) {
    def storageCost: Cost = a.map(a => gm.toByteArray(a).size).sum
  }
}
