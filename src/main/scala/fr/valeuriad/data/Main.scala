package fr.valeuriad.data
import fr.valeuriad.data.common.{AppConfig, SparkSessionProvider}
import fr.valeuriad.data.silver.SilverJob
import java.time.LocalDate


/**
 * Usage : Main <zone> [date_ingestion]
 *   zone           : silver (gold à venir)
 *   date_ingestion : yyyy-MM-dd, aujourd'hui par défaut
 */
object Main {
  def main(args: Array[String]): Unit = {
    val zone = args.headOption.getOrElse("silver")
    val ingestionDate = args.lift(1).map(LocalDate.parse).getOrElse(LocalDate.now())

    val config = AppConfig.load()
    val spark = SparkSessionProvider.create("tp_spark-job", config.sparkMaster)

    try {
      zone match {
        case "silver" => new SilverJob(spark, config).run(ingestionDate)
        case other    => throw new IllegalArgumentException(s"Zone inconnue : $other")
      }
    } finally {
      spark.stop()
    }
  }
}