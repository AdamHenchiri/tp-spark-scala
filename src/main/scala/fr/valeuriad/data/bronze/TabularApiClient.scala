package fr.valeuriad.data.bronze

import com.fasterxml.jackson.databind.ObjectMapper

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/** Une page de l'API tabulaire : lignes (en JSON brut), lien vers la page suivante, nombre total de lignes. */
case class Page(rows: Seq[String], next: Option[String], total: Long)

/**
 * API tabulaire data.gouv.fr : GET /resources/{rid}/data/?page_size=N
 * Renvoie les lignes par pages ; on suit le lien "next" jusqu'à la dernière.
 */
class TabularApiClient(tabularUrl: String, pageSize: Int, http: Http = new Http()) {

  def firstPageUrl(resourceId: String): String =
    s"$tabularUrl/resources/$resourceId/data/?page_size=$pageSize"

  /**
   * Parcourt toutes les pages et passe chaque page à `onPage` au fur et à mesure,
   * pour ne pas garder toutes les lignes en mémoire.
   */
  def foreachPage(resourceId: String)(onPage: Page => Unit): Unit = {
    @tailrec
    def loop(url: String): Unit = {
      val page = TabularApiClient.parsePage(http.get(url))
      onPage(page)
      page.next match {
        case Some(nextUrl) if page.rows.nonEmpty => loop(nextUrl)
        case _                                   => ()
      }
    }
    loop(firstPageUrl(resourceId))
  }
}

object TabularApiClient {

  private val mapper = new ObjectMapper()

  def parsePage(json: String): Page = {
    val root = mapper.readTree(json)
    val next = Option(root.path("links").path("next").asText(null)).filter(_.nonEmpty)
    Page(
      rows = root.path("data").elements().asScala.map(row => mapper.writeValueAsString(row)).toList,
      next = next,
      total = root.path("meta").path("total").asLong(0L)
    )
  }
}
