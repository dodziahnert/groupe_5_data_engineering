package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel
import com.ecommerce.utils.OptimizationConfig

object SparkOptimizations {

  def configureShufflePartitions(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.shuffle.partitions", OptimizationConfig.shufflePartitions)
  }

  def cacheIfEnabled(df: DataFrame): DataFrame =
    if (OptimizationConfig.enableCache) df.cache() else df

  def persistSerIfEnabled(df: DataFrame): DataFrame =
    if (OptimizationConfig.enableCache) df.persist(StorageLevel.MEMORY_AND_DISK_SER) else df

  def unpersistSafe(df: DataFrame): Unit =
    if (df.storageLevel != StorageLevel.NONE) df.unpersist()

  def broadcastIfEnabled(df: DataFrame): DataFrame =
    if (OptimizationConfig.enableBroadcast) broadcast(df) else df
}
