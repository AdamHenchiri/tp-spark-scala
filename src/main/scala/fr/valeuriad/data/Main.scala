package fr.valeuriad.data

import fr.valeuriad.data.common.{AppConfig, DbConfig, JdbcWriter, SparkSessionProvider}
import fr.valeuriad.data.bronze.BronzeJob
import fr.valeuriad.data.gold.GoldJob
import fr.valeuriad.data.silver.SilverJob

import java.time.LocalDate

/**
 * Usage : Main <zone> [date_ingestion]
 *   zone           : bronze | silver | gold | all   (silver par défaut)
 *   date_ingestion : yyyy-MM-dd, aujourd'hui par défaut (utilisée par la zone silver)
 */
object Main {

  def main(args: Array[String]): Unit = {
    val zone = args.headOption.getOrElse("silver")
    val ingestionDate = args.lift(1).map(LocalDate.parse).getOrElse(LocalDate.now())

    val config = AppConfig.load()

    // La zone bronze n'utilise pas Spark : inutile de le démarrer
    if (zone == "bronze") {
      BronzeJob(config).run()
      return
    }

    val spark = SparkSessionProvider.create("tp_spark-job", config.sparkMaster)

    def silver(): Unit = new SilverJob(spark, config).run(ingestionDate)
    def gold(): Unit = new GoldJob(spark, config, new JdbcWriter(DbConfig.load())).run()

    try {
      zone match {
        case "silver" => silver()
        case "gold"   => gold()
        case "all"    =>
          BronzeJob(config).run()
          silver()
          gold()
        case other    => throw new IllegalArgumentException(s"Zone inconnue : $other (bronze | silver | gold | all)")
      }
    } finally {
      spark.stop()
    }
  }
}
