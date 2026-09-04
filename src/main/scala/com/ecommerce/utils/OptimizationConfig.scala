package com.ecommerce.utils

import com.typesafe.config.ConfigFactory

object OptimizationConfig {
  private val conf = ConfigFactory.load()

  val shufflePartitions: Int =
    if (conf.hasPath("app.spark.shuffle-partitions")) conf.getInt("app.spark.shuffle-partitions") else 8

  val enableCache: Boolean =
    if (conf.hasPath("optimization.enable-cache")) conf.getBoolean("optimization.enable-cache") else true

  val enableBroadcast: Boolean =
    if (conf.hasPath("optimization.enable-broadcast")) conf.getBoolean("optimization.enable-broadcast") else true
}
