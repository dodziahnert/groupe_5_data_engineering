package com.ecommerce.models

import org.apache.spark.sql.DataFrame

// resultat d'une validation : les lignes valides et les lignes rejetees
case class ValidationResult(
  valid: DataFrame,      // lignes qui respectent toutes les regles
  rejected: DataFrame    // lignes rejetees, avec la colonne rejection_reason
)
