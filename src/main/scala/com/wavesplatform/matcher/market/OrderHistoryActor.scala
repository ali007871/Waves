package com.wavesplatform.matcher.market

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.StatusCodes
import com.wavesplatform.matcher.MatcherSettings
import com.wavesplatform.matcher.api.{BadMatcherResponse, MatcherResponse}
import com.wavesplatform.matcher.market.OrderBookActor.{CancelOrder, GetOrderStatusResponse}
import com.wavesplatform.matcher.market.OrderHistoryActor._
import com.wavesplatform.matcher.model.Events.{Event, OrderAdded, OrderCanceled, OrderExecuted}
import com.wavesplatform.matcher.model.LimitOrder.Filled
import com.wavesplatform.matcher.model._
import com.wavesplatform.utils
import org.h2.mvstore.MVStore
import play.api.libs.json._
import scorex.account.Account
import scorex.transaction.AssetAcc
import scorex.transaction.ValidationError.CustomValidationError
import scorex.transaction.assets.exchange.{AssetPair, Order}
import scorex.transaction.state.database.blockchain.StoredState
import scorex.utils.NTP
import scorex.wallet.Wallet

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class OrderHistoryActor(val settings: MatcherSettings, val storedState: StoredState, val wallet: Wallet)
  extends Actor with OrderValidator {
  val RequestTTL: Int = 5*1000
  val UpdateOpenPortfolioDelay: FiniteDuration = 30 seconds

  val db: MVStore = utils.createMVStore(settings.orderHistoryFile)
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

  def processExpirableRequest(r: Any): Unit = r match {
    case req: GetOrderHistory =>
      fetchOrderHistory(req)
    case req: GetAllOrderHistory =>
      fetchAllOrderHistory(req)
    case ValidateOrder(o, ts) =>
      sender() ! ValidateOrderResult(validateNewOrder(o))
    case ValidateCancelOrder(co, _) =>
      sender() ! ValidateCancelResult(validateCancelOrder(co))
    case req: DeleteOrderFromHistory =>
      deleteFromOrderHistory(req)
    case GetOrderStatus(_, id, _) =>
      sender() ! GetOrderStatusResponse(orderHistory.getOrderStatus(id))
    case GetTradableBalance(assetPair, addr, _) =>
      sender() ! getPairTradableBalance(assetPair, addr)
  }

  override def receive: Receive = {
    case req: ExpirableOrderHistoryRequest =>
      if (NTP.correctedTime() - req.ts < RequestTTL) {
        processExpirableRequest(req)
      }
    case ev: OrderAdded =>
      orderHistory.didOrderAccepted(ev)
    case ev: OrderExecuted =>
      orderHistory.didOrderExecutedUnconfirmed(ev)
      context.system.scheduler.scheduleOnce(UpdateOpenPortfolioDelay, self, UpdateOpenPortfolio(ev))
    case ev: OrderCanceled =>
      orderHistory.didOrderCanceled(ev)
    case RecoverFromOrderBook(ob) =>
      recoverFromOrderBook(ob)
    case UpdateOpenPortfolio(ev) =>
      orderHistory.saveOpenPortfolio(ev)
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
  sealed trait ExpirableOrderHistoryRequest extends OrderHistoryRequest {
    def ts: Long
  }
  case class GetOrderHistory(assetPair: AssetPair, address: String, ts: Long) extends ExpirableOrderHistoryRequest
  case class GetAllOrderHistory(address: String, ts: Long) extends ExpirableOrderHistoryRequest
  case class GetOrderStatus(assetPair: AssetPair, id: String, ts: Long) extends ExpirableOrderHistoryRequest
  case class DeleteOrderFromHistory(assetPair: AssetPair, address: String, id: String, ts: Long) extends ExpirableOrderHistoryRequest
  case class ValidateOrder(order: Order, ts: Long) extends ExpirableOrderHistoryRequest
  case class ValidateOrderResult(result: Either[CustomValidationError, Order])
  case class ValidateCancelOrder(cancel: CancelOrder, ts: Long) extends ExpirableOrderHistoryRequest
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

  case class GetTradableBalance(assetPair: AssetPair, address: String, ts: Long) extends ExpirableOrderHistoryRequest
  case class GetTradableBalanceResponse(balances: Map[String, Long]) extends MatcherResponse {
    val json: JsObject = JsObject(balances.map{ case (k, v) => (k, JsNumber(v)) })
    val code = StatusCodes.OK
  }

  case class UpdateOpenPortfolio(event: Event)
}