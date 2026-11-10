package fr.valeuriad.data.gold

import fr.valeuriad.data.common.SparkTestSession
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite

class GoldTransformationsTest extends AnyFunSuite with SparkTestSession {

  import spark.implicits._
  import GoldTransformations._

  private def academies: DataFrame =
    Seq(("Paris", "01", "11"), ("Lyon", "10", "84"))
      .toDF("libelle_aca", "code_academie", "code_region_2016")

  // ---------- Table 1 ----------

  test("etablissementsParType compte les établissements sièges distincts par académie et type") {
    val etab = Seq(
      ("01", "Université", "U1"),
      ("01", "Université", "U1"), // 2e implantation du même établissement : compté une fois
      ("01", "Université", "U2"),
      ("01", "École", "E1"),
      ("10", "Université", "U3"),
      (null, "Université", "U4")  // sans académie : ignoré
    ).toDF("code_academie", "type_de_l_etablissement_siege", "code_de_l_etablissement_siege")

    val result = etablissementsParType(etab, academies)
      .as[(String, String, String, String, Long)].collect().toSet

    assert(result == Set(
      ("Paris", "01", "11", "Université", 2L),
      ("Paris", "01", "11", "École", 1L),
      ("Lyon", "10", "84", "Université", 1L)
    ))
  }

  // ---------- Table 2 ----------

  private def bac: DataFrame = Seq(
    // session, académie, sexe, voie, présents, admis 1er groupe, admis 2nd groupe
    (2021, "01", "Filles", "Générale", 100, 80, 10),
    (2021, "01", "Garçons", "Générale", 100, 60, 10),
    (2021, "10", "Filles", "Générale", 200, 150, 30),
    (2021, "10", "Garçons", "Générale", 100, 50, 10),
    (2021, "01", "Filles", "Professionnelle", 1000, 0, 0) // autre voie : exclue
  ).toDF("session", "code_academie", "sexe", "voie", "nombre_de_presents",
    "nombre_d_admis_au_1er_groupe", "nombre_d_admis_a_l_issue_du_2nd_groupe")

  test("filtreVoieGenerale ne garde que la voie générale, quelle que soit l'écriture") {
    val df = Seq("Générale", "GENERALE", "Voie générale", "Technologique", "Professionnelle").toDF("voie")
    assert(filtreVoieGenerale(df).as[String].collect().toSet == Set("Générale", "GENERALE", "Voie générale"))
  }

  test("pourcentage renvoie null si aucun présent") {
    val r = Seq((5L, 0L)).toDF("a", "p").select(pourcentage($"a", $"p")).head()
    assert(r.isNullAt(0))
  }

  test("tauxReussiteBac calcule les taux par genre, académie et national") {
    val result = tauxReussiteBac(bac, academies)
      .collect()
      .map(r => (r.getAs[String]("code_academie"), r.getAs[String]("genre")) -> r)
      .toMap

    val parisFilles: Row = result(("01", "Filles"))
    assert(parisFilles.getAs[String]("libelle_aca") == "Paris")
    assert(parisFilles.getAs[String]("code_region_2016") == "11")
    // (80 + 10) / 100
    assert(parisFilles.getAs[Double](Taux) == 90.0)
    // Paris, tous genres : (90 + 70) / 200
    assert(parisFilles.getAs[Double](TauxAcademie) == 80.0)
    // National filles : (90 + 180) / 300
    assert(parisFilles.getAs[Double](TauxNationalGenre) == 90.0)
    // National tous genres : (90 + 70 + 180 + 60) / 500
    assert(parisFilles.getAs[Double](TauxNational) == 80.0)

    val lyonGarcons = result(("10", "Garçons"))
    assert(lyonGarcons.getAs[Double](Taux) == 60.0)
    // National garçons : (70 + 60) / 200
    assert(lyonGarcons.getAs[Double](TauxNationalGenre) == 65.0)

    assert(result.size == 4) // la voie professionnelle n'apparaît pas
  }
}
