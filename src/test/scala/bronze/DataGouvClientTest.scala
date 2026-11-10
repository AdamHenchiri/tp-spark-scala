package fr.valeuriad.data.bronze

import org.scalatest.funsuite.AnyFunSuite

class DataGouvClientTest extends AnyFunSuite {

  private val json =
    """{
      |  "title": "Le baccalauréat par académie",
      |  "resources": [
      |    { "id": "r1", "title": "Export JSON",      "format": "json" },
      |    { "id": "r2", "title": "Export CSV",       "format": "CSV" },
      |    { "id": "r3", "title": "Données 2021 CSV", "format": "csv" }
      |  ]
      |}""".stripMargin

  test("parseResources lit id, titre et format (en minuscules)") {
    assert(DataGouvClient.parseResources(json) == Seq(
      Resource("r1", "Export JSON", "json"),
      Resource("r2", "Export CSV", "csv"),
      Resource("r3", "Données 2021 CSV", "csv")
    ))
  }

  test("chooseCsv prend le premier CSV, ou celui dont le titre correspond au filtre") {
    val resources = DataGouvClient.parseResources(json)
    assert(DataGouvClient.chooseCsv(resources, None).map(_.id).contains("r2"))
    assert(DataGouvClient.chooseCsv(resources, Some("2021")).map(_.id).contains("r3"))
    assert(DataGouvClient.chooseCsv(resources, Some("inexistant")).isEmpty)
  }

  test("parseResources renvoie une liste vide si le JSON n'a pas de ressources") {
    assert(DataGouvClient.parseResources("""{"title": "vide"}""").isEmpty)
  }
}
