package fr.valeuriad.data.common

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConverters._

/** Lit un fichier .env (CLE=valeur, une par ligne). Le fichier est optionnel. */
object DotEnv {

  def parse(lines: Seq[String]): Map[String, String] =
    lines
      .map(_.trim)
      .filterNot(l => l.isEmpty || l.startsWith("#"))
      .map(_.stripPrefix("export ").trim)
      .filter(_.contains("="))
      .map { l =>
        val (key, value) = l.splitAt(l.indexOf('='))
        key.trim -> unquote(value.drop(1).trim)
      }
      .toMap

  def load(path: Path = Paths.get(".env")): Map[String, String] =
    if (Files.exists(path)) parse(Files.readAllLines(path, StandardCharsets.UTF_8).asScala.toSeq)
    else Map.empty

  private def unquote(v: String): String =
    if (v.length >= 2 && ((v.startsWith("\"") && v.endsWith("\"")) || (v.startsWith("'") && v.endsWith("'"))))
      v.substring(1, v.length - 1)
    else v
}
