package com.wavesplatform.matcher.market

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.persistence.{PersistentActor, RecoveryCompleted}
import com.wavesplatform.UtxPool
import com.wavesplatform.matcher.MatcherSettings
import com.wavesplatform.matcher.api.{MatcherResponse, StatusCodeMatcherResponse}
import com.wavesplatform.matcher.market.OrderBookActor.{DeleteOrderBookRequest, GetOrderBookResponse, OrderBookRequest}
import com.wavesplatform.settings.FunctionalitySettings
import com.wavesplatform.state2.reader.StateReader
import io.netty.channel.group.ChannelGroup
import play.api.libs.json.{JsArray, JsValue, Json}
import scorex.crypto.encode.Base58
import scorex.transaction.assets.IssueTransaction
import scorex.transaction.assets.exchange.Validation.booleanOperators
import scorex.transaction.assets.exchange.{AssetPair, Order, Validation}
import scorex.transaction.{AssetId, History}
import scorex.utils._
import scorex.wallet.Wallet

import scala.collection.{immutable, mutable}
import scala.language.reflectiveCalls

class MatcherActor(orderHistory: ActorRef, storedState: StateReader, wallet: Wallet, utx: UtxPool, allChannels: ChannelGroup,
                   settings: MatcherSettings, history: History, functionalitySettings: FunctionalitySettings)
  extends PersistentActor with ScorexLogging {

  import MatcherActor._

  val openMarkets: mutable.Buffer[MarketData] = mutable.Buffer.empty[MarketData]
  val tradedPairs: mutable.Buffer[AssetPair] = mutable.Buffer.empty[AssetPair]

  def createOrderBook(pair: AssetPair): ActorRef = {
    def getAssetName(asset: Option[AssetId]): String = asset.map(storedState.getAssetName).getOrElse(AssetPair.WavesName)

    openMarkets += MarketData(pair, getAssetName(pair.amountAsset), getAssetName(pair.priceAsset), NTP.correctedTime(),
        pair.amountAsset.flatMap(storedState.getIssueTransaction), pair.priceAsset.flatMap(storedState.getIssueTransaction))
    tradedPairs += pair

    context.actorOf(OrderBookActor.props(pair, orderHistory, storedState, settings, wallet, utx, allChannels, history, functionalitySettings),
      OrderBookActor.name(pair))
  }

  def basicValidation(msg: {def assetPair: AssetPair}): Validation = {
    msg.assetPair.isValid :| "Invalid AssetPair" &&
      msg.assetPair.priceAsset.map(storedState.totalAssetQuantity).forall(_ > 0) :|
        s"Unknown Asset ID: ${msg.assetPair.priceAssetStr}" &&
      msg.assetPair.amountAsset.map(storedState.totalAssetQuantity).forall(_ > 0) :|
        s"Unknown Asset ID: ${msg.assetPair.amountAssetStr}"
  }

  def checkPairOrdering(aPair: AssetPair): Validation = {
    val reversePair = AssetPair(aPair.priceAsset, aPair.amountAsset)

    val isCorrectOrder = if (tradedPairs.contains(aPair)) true
    else if (tradedPairs.contains(reversePair)) false
    else if (settings.priceAssets.contains(aPair.priceAssetStr) &&
      !settings.priceAssets.contains(aPair.amountAssetStr)) true
    else if (settings.priceAssets.contains(reversePair.priceAssetStr) &&
      !settings.priceAssets.contains(reversePair.amountAssetStr)) false
    else compare(aPair.priceAsset.map(_.arr), aPair.amountAsset.map(_.arr)) < 0

    isCorrectOrder :| s"Invalid AssetPair ordering, should be reversed: $reversePair"
  }

  def createAndForward(order: Order): Unit = {
    val orderBook = createOrderBook(order.assetPair)
    persistAsync(OrderBookCreated(order.assetPair)) { _ =>
      forwardReq(order)(orderBook)
    }
  }

  def returnEmptyOrderBook(pair: AssetPair): Unit = {
    sender() ! GetOrderBookResponse(pair, Seq(), Seq())
  }

  def forwardReq(req: Any)(orderBook: ActorRef): Unit = orderBook forward req

  def checkAssetPair[A <: {def assetPair : AssetPair}](msg: A)(f: => Unit): Unit = {
    val v = basicValidation(msg)
    if (!v) {
      sender() ! StatusCodeMatcherResponse(StatusCodes.NotFound, v.messages())
    } else {
      val ov = checkPairOrdering(msg.assetPair)
      if (!ov) {
        sender() ! StatusCodeMatcherResponse(StatusCodes.Found, ov.messages())
      } else {
        f
      }
    }
  }

  def getMatcherPublicKey: Array[Byte] = {
    wallet.findWallet(settings.account).map(_.publicKey).getOrElse(Array())
  }

  def forwardToOrderBook: Receive = {
    case GetMarkets =>
      sender() ! GetMarketsResponse(getMatcherPublicKey, openMarkets)
    case order: Order =>
      checkAssetPair(order) {
        context.child(OrderBookActor.name(order.assetPair))
          .fold(createAndForward(order))(forwardReq(order))
      }
    case ob: DeleteOrderBookRequest =>
      checkAssetPair(ob) {
        context.child(OrderBookActor.name(ob.assetPair))
          .fold(returnEmptyOrderBook(ob.assetPair))(forwardReq(ob))
        removeOrderBook(ob.assetPair)
      }
    case ob: OrderBookRequest =>
      checkAssetPair(ob) {
        context.child(OrderBookActor.name(ob.assetPair))
          .fold(returnEmptyOrderBook(ob.assetPair))(forwardReq(ob))
      }
  }

  def initPredefinedPairs(): Unit = {
    settings.predefinedPairs.diff(tradedPairs).foreach(pair =>
      createOrderBook(pair)
    )
  }

  private def removeOrderBook(pair: AssetPair): Unit = {
    val i = tradedPairs.indexOf(pair)
    if (i >= 0) {
      openMarkets.remove(i)
      tradedPairs.remove(i)
      deleteMessages(lastSequenceNr)
      persistAll(tradedPairs.map(OrderBookCreated).to[immutable.Seq]) { _ => }
    }
  }

  override def receiveRecover: Receive = {
    case OrderBookCreated(pair) =>
      context.child(OrderBookActor.name(pair))
        .getOrElse(createOrderBook(pair))
    case RecoveryCompleted =>
      log.info("MatcherActor - Recovery completed!")
      initPredefinedPairs()
  }

  override def receiveCommand: Receive = forwardToOrderBook

  override def persistenceId: String = "matcher"
}

object MatcherActor {
  def name = "matcher"

  def props(orderHistoryActor: ActorRef, storedState: StateReader, wallet: Wallet, utx: UtxPool, allChannels: ChannelGroup,
            settings: MatcherSettings, history: History, functionalitySettings: FunctionalitySettings): Props =
    Props(new MatcherActor(orderHistoryActor, storedState, wallet, utx, allChannels,settings, history, functionalitySettings))

  case class OrderBookCreated(pair: AssetPair)

  case object GetMarkets

  case class GetMarketsResponse(publicKey: Array[Byte], markets: Seq[MarketData]) extends MatcherResponse {
    def getMarketsJs: JsValue = JsArray(markets.map(m => Json.obj(
      "amountAsset" -> m.pair.amountAssetStr,
      "amountAssetName" -> m.amountAssetName,
      "amountAssetInfo" -> m.amountAssetInfo.map(_.json),
      "priceAsset" -> m.pair.priceAssetStr,
      "priceAssetName" -> m.priceAssetName,
      "priceAssetInfo" -> m.priceAssetinfo.map(_.json),
      "created" -> m.created
    ))
    )

    def json: JsValue = Json.obj(
      "matcherPublicKey" -> Base58.encode(publicKey),
      "markets" -> getMarketsJs
    )

    def code: StatusCode = StatusCodes.OK
  }

  case class MarketData(pair: AssetPair, amountAssetName: String, priceAssetName: String, created: Long,
                        amountAssetInfo: Option[IssueTransaction], priceAssetinfo: Option[IssueTransaction])

  def compare(buffer1: Option[Array[Byte]], buffer2: Option[Array[Byte]]): Int = {
    if (buffer1.isEmpty && buffer2.isEmpty) 0
    else if (buffer1.isEmpty) -1
    else if (buffer2.isEmpty) 1
    else ByteArray.compare(buffer1.get, buffer2.get)
  }
}
