package com.wavesplatform.matcher.market

import java.io.File

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.StatusCodes
import com.wavesplatform.matcher.MatcherSettings
import com.wavesplatform.matcher.api.{BadMatcherResponse, MatcherResponse}
import com.wavesplatform.matcher.market.OrderBookActor.{CancelOrder, GetOrderStatusResponse}
import com.wavesplatform.matcher.market.OrderHistoryActor._
import com.wavesplatform.matcher.model.Events.{OrderAdded, OrderCanceled, OrderExecuted}
import com.wavesplatform.matcher.model.LimitOrder.Filled
import com.wavesplatform.matcher.model._
import org.h2.mvstore.MVStore
import play.api.libs.json._
import scorex.account.Account
import scorex.transaction.AssetAcc
import scorex.transaction.ValidationError.CustomValidationError
import scorex.transaction.assets.exchange.{AssetPair, Order}
import scorex.transaction.state.database.blockchain.StoredState
import scorex.wallet.Wallet

class OrderHistoryActor(val settings: MatcherSettings, val storedState: StoredState, val wallet: Wallet)
  extends Actor with OrderValidator {

  def createMVStore(fileName: String): MVStore = {
    Option(fileName).filter(_.trim.nonEmpty) match {
      case Some(s) =>
        val file = new File(s)
        file.getParentFile.mkdirs().ensuring(file.getParentFile.exists())

        val db = new MVStore.Builder().fileName(s).open()
        db.rollback()
        db
      case None =>
        new MVStore.Builder().open()
    }
  }

  val db: MVStore = createMVStore(settings.orderHistoryFile)
  val storage = new OrderHistoryStorage(db)
  val orderHistory = OrderHistoryImpl(storage)

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, classOf[OrderAdded])
    context.system.eventStream.subscribe(self, classOf[OrderExecuted])
    context.system.eventStream.subscribe(self, classOf[OrderCanceled])
  }

  override def postStop(): Unit = {
    db.commit()
    db.close()
  }

  override def receive: Receive = {
    case ev: OrderAdded =>
      orderHistory.didOrderAccepted(ev)
    case ev: OrderExecuted =>
      orderHistory.didOrderExecuted(ev)
    case ev: OrderCanceled =>
      orderHistory.didOrderCanceled(ev)
    case req: GetOrderHistory =>
      fetchOrderHistory(req)
    case req: GetAllOrderHistory =>
      fetchAllOrderHistory(req)
    case ValidateOrder(o) =>
      sender() ! ValidateOrderResult(validateNewOrder(o))
    case ValidateCancelOrder(co) =>
      sender() ! ValidateCancelResult(validateCancelOrder(co))
    case req: DeleteOrderFromHistory =>
      deleteFromOrderHistory(req)
    case GetOrderStatus(_, id) =>
      sender() ! GetOrderStatusResponse(orderHistory.getOrderStatus(id))
    case RecoverFromOrderBook(ob) =>
      recoverFromOrderBook(ob)
    case GetTradableBalance(assetPair, addr) =>
      sender() ! getPairTradableBalance(assetPair, addr)
  }

  def fetchOrderHistory(req: GetOrderHistory): Unit = {
    val res: Seq[(String, OrderInfo, Option[Order])] =
      orderHistory.getOrdersByPairAndAddress(req.assetPair, req.address)
        .map(id => (id, orderHistory.getOrderInfo(id), orderHistory.getOrder(id))).toSeq.sortBy(_._3.map(_.timestamp).getOrElse(-1L))
    sender() ! GetOrderHistoryResponse(res)
  }

  def fetchAllOrderHistory(req: GetAllOrderHistory): Unit = {
    val res: Seq[(String, OrderInfo, Option[Order])] =
      orderHistory.getAllOrdersByAddress(req.address)
        .map(id => (id, orderHistory.getOrderInfo(id), orderHistory.getOrder(id))).toSeq.sortBy(_._3.map(_.timestamp).getOrElse(-1L))
    sender() ! GetOrderHistoryResponse(res)
  }

  def getPairTradableBalance(assetPair: AssetPair, address: String): GetTradableBalanceResponse = {
    GetTradableBalanceResponse(Map(
      assetPair.amountAssetStr -> getTradableBalance(AssetAcc(new Account(address), assetPair.amountAsset)),
      assetPair.priceAssetStr -> getTradableBalance(AssetAcc(new Account(address), assetPair.priceAsset))
    ))
  }

  def deleteFromOrderHistory(req: DeleteOrderFromHistory): Unit = {
    orderHistory.getOrderStatus(req.id) match {
      case Filled | LimitOrder.Cancelled(_) =>
        orderHistory.deleteOrder(req.assetPair, req.address, req.id)
        sender() ! OrderDeleted(req.id)
      case _ =>
        sender() ! BadMatcherResponse(StatusCodes.BadRequest, "Order couldn't be deleted")
    }
  }

  def recoverFromOrderBook(ob: OrderBook): Unit = {
    ob.asks.foreach{ case (_, orders) =>
      orders.foreach(o => orderHistory.didOrderAccepted(OrderAdded(o)))
    }
    ob.bids.foreach{ case (_, orders) =>
      orders.foreach(o => orderHistory.didOrderAccepted(OrderAdded(o)))
    }
  }

}

object OrderHistoryActor {
  def name = "OrderHistory"
  def props(settings: MatcherSettings, storedState: StoredState, wallet: Wallet): Props =
    Props(new OrderHistoryActor(settings, storedState, wallet))

  sealed trait OrderHistoryRequest
  case class GetOrderHistory(assetPair: AssetPair, address: String) extends OrderHistoryRequest
  case class GetAllOrderHistory(address: String) extends OrderHistoryRequest
  case class GetOrderStatus(assetPair: AssetPair, id: String) extends OrderHistoryRequest
  case class DeleteOrderFromHistory(assetPair: AssetPair, address: String, id: String) extends OrderHistoryRequest
  case class ValidateOrder(order: Order) extends OrderHistoryRequest
  case class ValidateOrderResult(result: Either[CustomValidationError, Order])
  case class ValidateCancelOrder(cancel: CancelOrder) extends OrderHistoryRequest
  case class ValidateCancelResult(result: Either[CustomValidationError, CancelOrder])
  case class RecoverFromOrderBook(ob: OrderBook) extends OrderHistoryRequest

  case class OrderDeleted(orderId: String) extends MatcherResponse {
    val json = Json.obj("status" -> "OrderDeleted", "orderId" -> orderId)
    val code = StatusCodes.OK
  }

  case class GetOrderHistoryResponse(history: Seq[(String, OrderInfo, Option[Order])]) extends MatcherResponse {
    val json = JsArray(history.map(h => Json.obj(
      "id" -> h._1,
      "type" -> h._3.map(_.orderType.toString),
      "amount" -> h._2.amount,
      "price" -> h._3.map(_.price),
      "timestamp" -> h._3.map(_.timestamp),
      "filled" -> h._2.filled,
      "status" -> h._2.status.name,
      "assetPair" -> h._3.map(_.assetPair.json)
    )))
    val code = StatusCodes.OK
  }

  case class GetTradableBalance(assetPair: AssetPair, address: String) extends OrderHistoryRequest
  case class GetTradableBalanceResponse(balances: Map[String, Long]) extends MatcherResponse {
    val json: JsObject = JsObject(balances.map{ case (k, v) => (k, JsNumber(v)) })
    val code = StatusCodes.OK
  }

}