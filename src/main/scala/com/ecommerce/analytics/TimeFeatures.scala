package com.ecommerce.analytics

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util.Locale
import org.apache.spark.sql.functions.udf

// 1. Définition de la structure de données de sortie (Schéma)
case class TimeFeatureValues(
  hour: Option[Int],
  day_of_week: Option[String],
  month: Option[String],
  is_weekend: Int,
  day_period: Option[String],
  is_working_hours: Int
)

object TimeFeatures {

  // 1.1 Définition du format de date attendu (yyyyMMddHHmmss) pour le parsing
  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss", Locale.ENGLISH)

  // 2. Définition de l'UDF 
  val extractTimeFeatures = udf((rawDate: String) => {
    
    // Règle de ROBUSTESSE : gestion des nulls, chaînes vides ou mal formatées
    if (rawDate == null || rawDate.trim.isEmpty || !rawDate.matches("\\d{14}")) {
      // Valeurs par défaut en cas d'erreur
      TimeFeatureValues(None, None, None, 0, None, 0)
    } else {
      try {
        // Parsing de la date
        val dt = LocalDateTime.parse(rawDate, formatter)
        
        val h = dt.getHour
        val dayOfWeek = dt.getDayOfWeek
        val month = dt.getMonth

        // Détermination de la période de la journée
        val dayPeriod = h match {
          case hour if hour >= 6 && hour < 12  => "Morning"
          case hour if hour >= 12 && hour < 18 => "Afternoon"
          case hour if hour >= 18 && hour < 22 => "Evening"
          case _                               => "Night" // Gère [22-23] et [0-5]
        }

        // Construction du résultat
        TimeFeatureValues(
          hour = Some(h),
          day_of_week = Some(dayOfWeek.getDisplayName(java.time.format.TextStyle.FULL, Locale.ENGLISH)),
          month = Some(month.getDisplayName(java.time.format.TextStyle.FULL, Locale.ENGLISH)),
          is_weekend = if (dayOfWeek.getValue >= 6) 1 else 0, // 6 = Samedi, 7 = Dimanche
          day_period = Some(dayPeriod),
          is_working_hours = if (h >= 9 && h < 17) 1 else 0
        )

      } catch {
        // ROBUSTESSE : Si le parsing échoue (ex: 20239999256060 -> date impossible)
        case _: DateTimeParseException => TimeFeatureValues(None, None, None, 0, None, 0)
      }
    }
  })
}