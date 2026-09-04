package com.ecommerce.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Analytics {

  def merchantReport(enriched: DataFrame): DataFrame = {
    val agg = enriched.groupBy("merchant_id").agg(
      first("merchant_name").as("merchant_name"),
      first("merchant_category").as("category"),
      first("merchant_region").as("region"),
      first("commission_rate").as("commission_rate"),
      sum("amount").as("total_revenue"),
      count("transaction_id").as("nb_transactions"),
      countDistinct("user_id").as("nb_unique_customers"),
      round(avg("amount"), 2).as("avg_transaction_amount"),
      sum(col("is_suspicious")).as("nb_transactions_suspectes")
    ).withColumn("total_commission", round(col("total_revenue") * col("commission_rate"), 2))

    val byCategory = Window.partitionBy("category").orderBy(col("total_revenue").desc)
    val byRegion   = Window.partitionBy("region").orderBy(col("total_revenue").desc)

    agg
      .withColumn("rank_in_category", rank().over(byCategory))
      .withColumn("rank_in_region", rank().over(byRegion))
  }

  def merchantAgeDistribution(enriched: DataFrame): DataFrame =
    enriched
      .groupBy("merchant_id", "age_group")
      .agg(
        sum("amount").as("revenue_for_bracket"),
        count("transaction_id").as("nb_transactions_for_bracket")
      )

  private def withCohortMonth(df: DataFrame): DataFrame = {
    val w = Window.partitionBy("user_id")
    df.withColumn("cohort_month", date_format(min("transaction_time").over(w), "yyyy-MM"))
      .withColumn("tx_month", date_format(col("transaction_time"), "yyyy-MM"))
  }

  def cohortRetentionMatrix(enriched: DataFrame): DataFrame = {
    val withCohort = withCohortMonth(enriched)
      .withColumn("period_index",
        round(months_between(
          to_date(concat(col("tx_month"), lit("-01"))),
          to_date(concat(col("cohort_month"), lit("-01")))
        )).cast("int"))

    val cohortSizes = withCohort
      .filter(col("period_index") === 0)
      .groupBy("cohort_month")
      .agg(countDistinct("user_id").as("cohort_initial_size"))

    val activity = withCohort
      .groupBy("cohort_month", "period_index")
      .agg(countDistinct("user_id").as("active_users"))

    activity.join(cohortSizes, Seq("cohort_month"))
      .withColumn("retention_rate_pct",
        round(col("active_users") * 100.0 / col("cohort_initial_size"), 2))
      .orderBy("cohort_month", "period_index")
  }

  def bestCohortAt3Months(retentionMatrix: DataFrame): DataFrame =
    retentionMatrix
      .filter(col("period_index") === 3)
      .orderBy(col("retention_rate_pct").desc)
      .limit(1)

  def fraudByMerchant(enriched: DataFrame): DataFrame =
    enriched
      .filter(col("is_suspicious") === 1)
      .groupBy("merchant_id")
      .agg(
        first("merchant_name").as("merchant_name"),
        count("transaction_id").as("nb_transactions_suspectes"),
        sum("amount").as("montant_total_suspect")
      )
      .orderBy(col("nb_transactions_suspectes").desc)
}
