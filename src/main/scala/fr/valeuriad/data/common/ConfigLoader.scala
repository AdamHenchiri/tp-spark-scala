package fr.valeuriad.data.common

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._

/**
 * application.conf + valeurs du fichier .env.
 * Les clés du .env (DB_USER, DB_PASSWORD...) alimentent les ${?DB_USER} de application.conf.
 */
object ConfigLoader {

  def load(dotEnv: Map[String, String] = DotEnv.load()): Config =
    ConfigFactory.load(
      ConfigFactory.parseMap(dotEnv.asJava).withFallback(ConfigFactory.defaultApplication())
    )
}
