package fr.valeuriad.data.bronze

import org.scalatest.funsuite.AnyFunSuite

class TabularApiClientTest extends AnyFunSuite {

  private def page(rows: String, next: String): String =
    s"""{ "data": [$rows], "links": { "next": $next }, "meta": { "page": 1, "page_size": 2, "total": 3 } }"""

  /** Faux HTTP : renvoie des pages prédéfinies selon l'URL, sans réseau. */
  private class FakeHttp(pages: Map[String, String]) extends Http() {
    var calls: List[String] = Nil
    override def get(url: String): String = { calls :+= url; pages(url) }
  }

  test("parsePage lit les lignes, le lien suivant et le total") {
    val p = TabularApiClient.parsePage(page("""{"__id": 1, "Session": 2021}""", "\"https://api/page2\""))
    assert(p.rows == Seq("""{"__id":1,"Session":2021}"""))
    assert(p.next.contains("https://api/page2"))
    assert(p.total == 3)
  }

  test("parsePage : pas de page suivante quand next vaut null") {
    assert(TabularApiClient.parsePage(page("", "null")).next.isEmpty)
  }

  test("foreachPage suit les liens next jusqu'à la dernière page") {
    val http = new FakeHttp(Map(
      "https://api/resources/rid/data/?page_size=2" -> page("""{"a": 1}, {"a": 2}""", "\"https://api/p2\""),
      "https://api/p2" -> page("""{"a": 3}""", "null")
    ))
    var rows = Seq.empty[String]
    new TabularApiClient("https://api", 2, http).foreachPage("rid")(p => rows ++= p.rows)

    assert(rows == Seq("""{"a":1}""", """{"a":2}""", """{"a":3}"""))
    assert(http.calls.size == 2)
  }
}
