package fr.valeuriad.data.common

import org.scalatest.funsuite.AnyFunSuite

class DotEnvTest extends AnyFunSuite {

  test("parse lit les clés, ignore commentaires et lignes vides, retire guillemets et export") {
    val lines = Seq(
      "# identifiants Oracle",
      "",
      "DB_USER=adam",
      "export DB_PASSWORD=\"mot=de;passe\"",
      "DB_URL = 'jdbc:oracle:thin:@//host:1521/SERVICE'",
      "ligne sans egal"
    )
    assert(DotEnv.parse(lines) == Map(
      "DB_USER" -> "adam",
      "DB_PASSWORD" -> "mot=de;passe",
      "DB_URL" -> "jdbc:oracle:thin:@//host:1521/SERVICE"
    ))
  }

  test("les valeurs du .env sont injectées dans la config de la base") {
    val db = DbConfig.load(ConfigLoader.load(Map("DB_USER" -> "adam", "DB_PASSWORD" -> "secret")))
    assert(db.user == "adam")
    assert(db.password == "secret")
    assert(db.driver == "oracle.jdbc.OracleDriver")
  }
}
