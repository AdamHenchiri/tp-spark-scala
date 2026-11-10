package fr.valeuriad.data.common

import com.typesafe.config.Config
import scala.collection.JavaConverters._

// Cette classe permet de creer un objet AppConfig à partir d'un fichier de configuration (application.conf)
//grace à cette classe, on peut accéder aux paramètres de configuration de manière typée et centralisée dans 
//l'application sans jamais écrire de un chemin ou un nom de fichier en dur.

case class DatasetConfig(
    name: String,
    file: String,
    sep: String,
    format: String = "csv",
    resourceId: Option[String] = None,
    slug: Option[String] = None,
    resourceTitle: Option[String] = None
)

case class ApiConfig(
    baseUrl: String = "https://www.data.gouv.fr/api/1",
    tabularUrl: String = "https://tabular-api.data.gouv.fr/api",
    pageSize: Int = 50
)

case class GoldConfig(etablissementsTable: String, bacTable: String)

case class DbConfig(url: String, user: String, password: String, driver: String)

case class AppConfig(
    sparkMaster: String,
    bronzePath: String,
    silverPath: String,
    datasets: Seq[DatasetConfig],
    api: ApiConfig = ApiConfig(),
    gold: GoldConfig = GoldConfig("gold_etablissements_par_type", "gold_taux_reussite_bac")
)

object AppConfig {

  def load(config: Config = ConfigLoader.load()): AppConfig = {
    val app = config.getConfig("app")
    AppConfig(
      sparkMaster = app.getString("spark.master"),
      bronzePath = app.getString("bronze.path"),
      silverPath = app.getString("silver.path"),
      datasets = app.getConfigList("datasets").asScala.map { d =>
        def opt(key: String): Option[String] = if (d.hasPath(key)) Some(d.getString(key)) else None
        DatasetConfig(d.getString("name"), d.getString("file"), d.getString("sep"),
          format = opt("format").getOrElse("csv"),
          resourceId = opt("resource-id"),
          slug = opt("slug"),
          resourceTitle = opt("resource-title"))
      }.toSeq,
      api = ApiConfig(
        baseUrl = app.getString("api.base-url"),
        tabularUrl = app.getString("api.tabular-url"),
        pageSize = app.getInt("api.page-size")
      ),
      gold = GoldConfig(
        etablissementsTable = app.getString("gold.etablissements.table"),
        bacTable = app.getString("gold.bac.table")
      )
    )
  }
}

object DbConfig {

  def load(config: Config = ConfigLoader.load()): DbConfig = {
    val db = config.getConfig("app.db")
    DbConfig(db.getString("url"), db.getString("user"), db.getString("password"), db.getString("driver"))
  }
}
