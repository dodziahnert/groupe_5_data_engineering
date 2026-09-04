package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DataTransformation {
    private val timestampFormat = "yyyyMMddHHmmss"
	
    def transform(
            transactions: DataFrame,
            users: DataFrame,
            products: DataFrame,
            merchants: DataFrame
    ): DataFrame = {
        val joined = transactions.alias("t")
            .join(users.alias("u"), Seq("user_id"), "inner")
            .join(products.alias("p"), Seq("product_id"), "left")
            .join(merchants.alias("m"), Seq("merchant_id"), "left")
            .select(
                col("t.*"),
                col("u.age"), col("u.annual_income"), col("u.city").alias("user_city"),
                col("u.customer_segment"), col("u.preferred_categories"), col("u.registration_date"),
                col("p.name").alias("product_name"), col("p.price").alias("product_price"),
                col("p.rating").alias("product_rating"), col("p.stock").alias("product_stock"),
                col("m.name").alias("merchant_name"), col("m.category").alias("merchant_category"),
                col("m.region").alias("merchant_region"), col("m.commission_rate"),
                col("m.establishment_date")
            )

		val cleanedJoined = joined.withColumn("clean_timestamp", substring(col("timestamp"), 1, 14))	
        
		val withTime = cleanedJoined
            .withColumn("time_features", TimeFeatures.extractTimeFeatures(col("clean_timestamp")))
            .withColumn("hour", col("time_features.hour"))
            .withColumn("day_of_week", col("time_features.day_of_week"))
            .withColumn("month", col("time_features.month"))
            .withColumn("is_weekend", col("time_features.is_weekend"))
            .withColumn("day_period", col("time_features.day_period"))
            .withColumn("is_working_hours", col("time_features.is_working_hours"))
            .drop("time_features")

        val parsed = withTime
            // Remplacement de try_to_timestamp par to_timestamp (pour compatibilité Spark < 3.3)
            .withColumn("transaction_time", to_timestamp(col("clean_timestamp"), timestampFormat))
            .withColumn("transaction_day", to_date(col("transaction_time")))
            .withColumn("transaction_epoch", unix_timestamp(col("transaction_time")))
			.drop("clean_timestamp")

        val userOrder = Window.partitionBy("user_id").orderBy(col("transaction_time"), col("transaction_id"))
        val userHistory = Window.partitionBy("user_id").orderBy(col("transaction_time"), col("transaction_id"))
            .rowsBetween(Window.unboundedPreceding, -1)
            
        // Cast explicite de transaction_epoch en Long pour la compatibilité stricte de rangeBetween
        val sevenDays = Window.partitionBy("user_id").orderBy(col("transaction_epoch").cast("long"))
            .rangeBetween(-7L * 24L * 60L * 60L, 0L)

        parsed
            .withColumn("transaction_rank", row_number().over(userOrder))
            .withColumn("total_transactions", count(lit(1)).over(Window.partitionBy("user_id")))
            .withColumn("age_group", when(col("age") < 26, "Jeune")
                .when(col("age").between(26, 44), "Adulte") 
                .when(col("age").between(45, 64), "Âge Moyen")
                .when(col("age") >= 65, "Senior")
                .otherwise("Inconnu")) // Ajout d'une sécurité
            .withColumn("rolling_7d_amount", sum(col("amount")).over(sevenDays))
            .withColumn("rolling_7d_distinct_days", size(collect_set(col("transaction_day")).over(sevenDays)))
            .withColumn("is_active_user", when(col("rolling_7d_distinct_days") >= 5, 1).otherwise(0))
            .withColumn("previous_transaction_time", lag(col("transaction_time"), 1).over(userOrder))
            .withColumn("days_since_previous_purchase",
                (unix_timestamp(col("transaction_time")) - unix_timestamp(col("previous_transaction_time"))) / 86400.0)
            .withColumn("historical_average_amount", avg(col("amount")).over(userHistory))
            .withColumn("basket_average_deviation_pct",
                when(col("historical_average_amount").isNull || col("historical_average_amount") === 0, lit(0.0))
                    .otherwise((col("amount") - col("historical_average_amount")) / col("historical_average_amount") * 100.0))
            .withColumn("is_suspicious",
                ((when(col("basket_average_deviation_pct") > 300, 1).otherwise(0) +
                    when(col("day_period") === "Night", 1).otherwise(0) +
                    when(unix_timestamp(col("transaction_time")) - unix_timestamp(col("previous_transaction_time")) < 300, 1).otherwise(0) +
                    when(upper(col("payment_method")) === "CRYPTO", 1).otherwise(0)) >= 2).cast("int"))
            .drop("transaction_epoch", "transaction_day")
    }

    def enrichTransactions(
            transactions: DataFrame,
            users: DataFrame,
            products: DataFrame,
            merchants: DataFrame
    ): DataFrame = transform(transactions, users, products, merchants)

    
    def reportSuspicious(transformed: DataFrame): Unit = {
        println(">>> Nombre de transactions suspectes")
        val suspiciousCount = transformed.filter(col("is_suspicious") === 1).count()
        println(suspiciousCount)
        
        println(">>> 20 montants les plus eleves parmi les transactions suspectes")
        transformed.filter(col("is_suspicious") === 1).orderBy(col("amount").desc).select("amount").show(20, false)
    }
}
