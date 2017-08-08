package com.wavesplatform.matcher

import java.io.File

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import scorex.transaction.assets.exchange.AssetPair

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class MatcherSettings(enable: Boolean,
                           account: String,
                           bindAddress: String,
                           port: Int,
                           minOrderFee: Long,
                           orderMatchTxFee: Long,
                           journalDataDir: String,
                           snapshotsDataDir: String,
                           snapshotsInterval: FiniteDuration,
                           maxOpenOrders: Int,
                           priceAssets: Seq[String],
                           predefinedPairs: Seq[AssetPair],
                           maxTimestampDiff: FiniteDuration,
                           orderHistoryFile: String,
                           isMigrateToNewOrderHistoryStorage: Boolean,
                           blacklistedAssets: Set[String]
                          )


object MatcherSettings {
  val configPath: String = "waves.matcher"

  def fromConfig(config: Config): MatcherSettings = {
    val enabled = config.as[Boolean](s"$configPath.enable")
    val account = config.as[String](s"$configPath.account")
    val bindAddress = config.as[String](s"$configPath.bind-address")
    val port = config.as[Int](s"$configPath.port")
    val minOrderFee = config.as[Long](s"$configPath.min-order-fee")
    val orderMatchTxFee = config.as[Long](s"$configPath.order-match-tx-fee")
    val journalDirectory = config.as[String](s"$configPath.journal-directory")
    val snapshotsDirectory = config.as[String](s"$configPath.snapshots-directory")
    val snapshotsInterval = config.as[FiniteDuration](s"$configPath.snapshots-interval")
    val maxOpenOrders = config.as[Int](s"$configPath.max-open-orders")
    val baseAssets = config.as[List[String]](s"$configPath.price-assets")
    val basePairs: Seq[AssetPair] = config.getConfigList(s"$configPath.predefined-pairs").asScala.map { p: Config =>
      AssetPair.createAssetPair(p.as[String]("amountAsset"), p.as[String]("priceAsset")).get
    }
    val maxTimestampDiff = config.as[FiniteDuration](s"$configPath.max-timestamp-diff")

    val orderHistoryFile = config.as[String](s"$configPath.order-history-file")

    val isMigrateToNewOrderHistoryStorage = !new File(orderHistoryFile).exists()

    val blacklistedAssets = config.as[List[String]](s"$configPath.blacklisted-assets")

    MatcherSettings(enabled, account, bindAddress, port, minOrderFee, orderMatchTxFee, journalDirectory,
      snapshotsDirectory, snapshotsInterval, maxOpenOrders, baseAssets, basePairs, maxTimestampDiff,
      orderHistoryFile, isMigrateToNewOrderHistoryStorage, blacklistedAssets.toSet)
  }
}
