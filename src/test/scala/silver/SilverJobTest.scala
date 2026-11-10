package fr.valeuriad.data.silver

import fr.valeuriad.data.common.{AppConfig, DatasetConfig, SparkTestSession}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}
import java.time.LocalDate

class SilverJobTest extends AnyFunSuite with SparkTestSession {

  test("les CSV Bronze sont écrits en Parquet Silver, avec la région ajoutée au bac") {
    val root: Path = Files.createTempDirectory("tp_spark")
    val bronze = Files.createDirectories(root.resolve("bronze"))
    Files.write(bronze.resolve("bac.csv"),
      "Session;Code académie;Sexe;Nombre d'admis totaux\n2021;A01;F;120\n".getBytes("UTF-8"))
    Files.write(bronze.resolve("aca.csv"),
      ("libelle_aca;libelle_region_2016;code_academie;code_region_2016;Geo Shape\n" +
        "Paris;Île-de-France;01;11;\"{\"\"type\"\": \"\"Polygon\"\"}\"\n").getBytes("UTF-8"))

    val config = AppConfig(
      sparkMaster = "local[2]",
      bronzePath = bronze.toString,
      silverPath = root.resolve("silver").toString,
      datasets = Seq(DatasetConfig("bac", "bac.csv", ";"), DatasetConfig("academies", "aca.csv", ";"))
    )

    new SilverJob(spark, config).run(LocalDate.of(2026, 11, 9))

    val bac = spark.read.parquet(s"${config.silverPath}/bac")
    assert(bac.columns.toSet == Set("code_academie", "session", "sexe", "nombre_d_admis_totaux",
      "date_ingestion", "libelle_region_2016", "code_region_2016"))
    assert(bac.select("libelle_region_2016").head().getString(0) == "Île-de-France")

    val academies = spark.read.parquet(s"${config.silverPath}/academies")
    assert(academies.select("geo_shape").head().getString(0) == "{\"type\": \"Polygon\"}")
  }
}
