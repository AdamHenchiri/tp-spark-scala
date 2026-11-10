package fr.valeuriad.data.bronze

import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConverters._

/** Une ressource (un fichier) d'un jeu de données data.gouv.fr. */
case class Resource(id: String, title: String, format: String)

/** API catalogue : sert seulement à retrouver l'id d'une ressource à partir du jeu de données. */
class DataGouvClient(baseUrl: String, http: Http = new Http()) {

  /** GET /datasets/{slug}/ */
  def resources(slug: String): Seq[Resource] =
    DataGouvClient.parseResources(http.get(s"$baseUrl/datasets/$slug/"))
}

object DataGouvClient {

  private val mapper = new ObjectMapper()

  def parseResources(json: String): Seq[Resource] =
    mapper.readTree(json).path("resources").elements().asScala.map { r =>
      Resource(
        id = r.path("id").asText(""),
        title = r.path("title").asText(""),
        format = r.path("format").asText("").toLowerCase
      )
    }.toList

  /** Premier CSV du jeu (l'API tabulaire expose les CSV), éventuellement filtré par son titre. */
  def chooseCsv(resources: Seq[Resource], titleFilter: Option[String]): Option[Resource] =
    resources
      .filter(_.format == "csv")
      .find(r => titleFilter.forall(f => r.title.toLowerCase.contains(f.toLowerCase)))
}
