package fr.valeuriad.data.common

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {

  def create(appName: String, master: String): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
      .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .getOrCreate()
}
