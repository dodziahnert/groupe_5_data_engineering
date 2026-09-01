package com.ecommerce.models

// moule d'un produit (fichier products.parquet)
case class Product(
  product_id: String,    // identifiant du produit
  name: String,          // nom du produit
  category: String,      // categorie
  price: Double,         // prix
  merchant_id: String,   // identifiant du marchand
  rating: Double,        // note moyenne
  stock: Int             // quantite en stock
)
