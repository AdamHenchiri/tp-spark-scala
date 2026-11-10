package fr.valeuriad.data.silver

import fr.valeuriad.data.common.SparkTestSession
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Date
import java.time.LocalDate

class SilverTransformationsTest extends AnyFunSuite with SparkTestSession {

  import spark.implicits._

  test("normalizeColumnName retire accents, espaces, apostrophes et majuscules") {
    assert(SilverTransformations.normalizeColumnName("Code académie") == "code_academie")
    assert(SilverTransformations.normalizeColumnName("Nombre d'admis au 1er groupe") == "nombre_d_admis_au_1er_groupe")
    assert(SilverTransformations.normalizeColumnName("type de l'établissement siège") == "type_de_l_etablissement_siege")
    assert(SilverTransformations.normalizeColumnName("Geo Shape") == "geo_shape")
    assert(SilverTransformations.normalizeColumnName("﻿Session") == "session")
  }

  test("normalizeColumnNames renomme toutes les colonnes du DataFrame") {
    val df = Seq(("A", 1)).toDF("Libellé académie", "Nombre d'admis totaux")
    assert(SilverTransformations.normalizeColumnNames(df).columns.toSeq == Seq("libelle_academie", "nombre_d_admis_totaux"))
  }

  test("normalizeAcademyCode uniformise les différents formats de code") {
    val result = Seq("1", "01", "A01", " a10 ", "28").toDF("c")
      .select(SilverTransformations.normalizeAcademyCode(col("c"))).as[String].collect()
    assert(result.toSeq == Seq("01", "01", "01", "10", "28"))
  }

  test("addIngestionDate ajoute la date sur chaque ligne") {
    val date = LocalDate.of(2026, 11, 9)
    val result = SilverTransformations.addIngestionDate(Seq("a", "b").toDF("x"), date)

    val dates = result.select(SilverTransformations.IngestionDateColumn).collect().map(_.getDate(0))
    assert(dates.length == 2)
    assert(dates.forall(_ == Date.valueOf(date)))
  }

  test("addRegion ajoute la région et garde les académies inconnues") {
    val bac = Seq(("01", 2021, 100), ("99", 2021, 50)).toDF("code_academie", "session", "nombre_d_admis_totaux")
    val academies = Seq(("Paris", "Île-de-France", "01", "11"))
      .toDF("libelle_aca", "libelle_region_2016", "code_academie", "code_region_2016")

    val result = SilverTransformations.addRegion(bac, academies)
      .select("code_academie", "libelle_region_2016", "code_region_2016")
      .as[(String, String, String)].collect()
      .map { case (c, l, r) => c -> (l, r) }.toMap

    assert(result("01") == ("Île-de-France", "11"))
    assert(result("99") == (null, null))
  }
}
