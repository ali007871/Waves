package scorex.transaction

import com.google.common.primitives.Ints
import play.api.libs.json.Json
import scorex.account.Account
import scorex.crypto.encode.Base58
import scorex.serialization.BytesSerializable
import scorex.transaction.TypedTransaction.TransactionType

import scala.concurrent.duration._


abstract class LagonakiTransaction(val transactionType: TransactionType.Value,
                                   val recipient: Account,
                                   val amount: Long,
                                   val fee: Long,
                                   override val timestamp: Long,
                                   override val signature: Array[Byte])
  extends TypedTransaction with BytesSerializable {

  import LagonakiTransaction._

  override val assetFee: (Option[AssetId], Long) = (None, fee)
  override val id: Array[Byte] = signature

  lazy val deadline = timestamp + 24.hours.toMillis

  lazy val hasMinimumFee = fee >= MinimumFee

  val TypeId = transactionType.id

  //PARSE/CONVERT
  val dataLength: Int

  val creator: Option[Account]


  val signatureValid: Boolean

  //VALIDATE
  def validate: ValidationResult.Value

  def involvedAmount(account: Account): Long

  override def equals(other: Any): Boolean = other match {
    case tx: LagonakiTransaction => signature.sameElements(tx.signature)
    case _ => false
  }

  override def hashCode(): Int = Ints.fromByteArray(signature)

  protected def jsonBase() = {
    Json.obj("type" -> transactionType.id,
      "fee" -> fee,
      "timestamp" -> timestamp,
      "signature" -> Base58.encode(this.signature)
    )
  }
}

object LagonakiTransaction {

  val MaxBytesPerToken = 512

  //MINIMUM FEE
  val MinimumFee = 1
  val RecipientLength = Account.AddressLength
  val TypeLength = 1
  val TimestampLength = 8
  val AmountLength = 8

  object ValidationResult extends Enumeration {
    type ValidationResult = Value

    val ValidateOke = Value(1)
    val InvalidAddress = Value(2)
    val NegativeAmount = Value(3)
    val InsufficientFee = Value(4)
    val NoBalance = Value(5)
  }


}
