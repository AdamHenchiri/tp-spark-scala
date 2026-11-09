package fr.valeuriad.data.common

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._

case class DatasetConfig(name: String, file: String, sep: String)

case class AppConfig(
    sparkMaster: String,
    bronzePath: String,
    silverPath: String,
    datasets: Seq[DatasetConfig]
)

object AppConfig {

  def load(config: Config = ConfigFactory.load()): AppConfig = {
    val app = config.getConfig("app")
    AppConfig(
      sparkMaster = app.getString("spark.master"),
      bronzePath = app.getString("bronze.path"),
      silverPath = app.getString("silver.path"),
      datasets = app.getConfigList("datasets").asScala.map { d =>
        DatasetConfig(d.getString("name"), d.getString("file"), d.getString("sep"))
      }.toSeq
    )
  }
}
