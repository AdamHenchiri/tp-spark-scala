package fr.valeuriad.data.common

import org.apache.spark.sql.SparkSession

/** SparkSession partagée par tous les tests. */
trait SparkTestSession {
  lazy val spark: SparkSession = SparkSessionProvider.create("tests", "local[2]")
}
