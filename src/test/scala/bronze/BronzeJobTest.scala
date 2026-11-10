package fr.valeuriad.data.bronze

import fr.valeuriad.data.common.{AppConfig, DatasetConfig}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.JavaConverters._

class BronzeJobTest extends AnyFunSuite {

  private class FakeCatalog(res: Seq[Resource]) extends DataGouvClient("http://inutilise") {
    override def resources(slug: String): Seq[Resource] = res
  }

  private class FakeTabular(pages: Seq[Page]) extends TabularApiClient("http://inutilise", 2) {
    var askedFor: Option[String] = None
    override def foreachPage(resourceId: String)(onPage: Page => Unit): Unit = {
      askedFor = Some(resourceId)
      pages.foreach(onPage)
    }
  }

  private val config = AppConfig("local[1]", "data/bronze", "data/silver", Nil)

  test("resolveResourceId utilise l'id de la config s'il est renseigné") {
    val job = new BronzeJob(config, new FakeCatalog(Nil), new FakeTabular(Nil))
    assert(job.resolveResourceId(DatasetConfig("bac", "bac.jsonl", ";", resourceId = Some("rid-42"))) == "rid-42")
  }

  test("resolveResourceId trouve l'id du CSV via le catalogue, sinon liste les ressources") {
    val catalog = new FakeCatalog(Seq(Resource("r1", "Export JSON", "json"), Resource("r2", "Export CSV", "csv")))
    val job = new BronzeJob(config, catalog, new FakeTabular(Nil))
    assert(job.resolveResourceId(DatasetConfig("bac", "bac.jsonl", ";", slug = Some("le-bac"))) == "r2")

    val error = intercept[IllegalStateException] {
      job.resolveResourceId(DatasetConfig("bac", "bac.jsonl", ";", slug = Some("le-bac"), resourceTitle = Some("2030")))
    }
    assert(error.getMessage.contains("Export JSON"))
  }

  test("ingest écrit toutes les lignes de toutes les pages en JSON Lines") {
    val dir = Files.createTempDirectory("bronze")
    val tabular = new FakeTabular(Seq(
      Page(Seq("""{"a":1}""", """{"a":2}"""), Some("p2"), 3),
      Page(Seq("""{"a":3}"""), None, 3)
    ))
    val job = new BronzeJob(config.copy(bronzePath = dir.toString), new FakeCatalog(Nil), tabular)

    val count = job.ingest(DatasetConfig("bac", "bac.jsonl", ";", resourceId = Some("rid")))

    assert(count == 3)
    assert(tabular.askedFor.contains("rid"))
    assert(Files.readAllLines(dir.resolve("bac.jsonl"), StandardCharsets.UTF_8).asScala ==
      Seq("""{"a":1}""", """{"a":2}""", """{"a":3}"""))
    assert(!Files.exists(dir.resolve("bac.jsonl.part")))
  }
}
