package fr.valeuriad.data.bronze

import fr.valeuriad.data.common.{AppConfig, DatasetConfig}
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

/**
 * API tabulaire data.gouv.fr -> Bronze.
 * Les lignes sont déposées telles que l'API les renvoie, au format JSON Lines (une ligne JSON par ligne de données).
 */
class BronzeJob(config: AppConfig, catalog: DataGouvClient, tabular: TabularApiClient) {

  private val log = LoggerFactory.getLogger(getClass)

  /** Id de la ressource : celui de la config, sinon le 1er CSV du jeu de données trouvé via le catalogue. */
  def resolveResourceId(dataset: DatasetConfig): String =
    dataset.resourceId.getOrElse {
      val slug = dataset.slug.getOrElse(
        throw new IllegalArgumentException(s"'${dataset.name}' : ni resource-id ni slug dans la config"))
      val resources = catalog.resources(slug)
      DataGouvClient.chooseCsv(resources, dataset.resourceTitle).map(_.id).getOrElse {
        val available = resources.map(r => s"  - [${r.format}] ${r.title} (id ${r.id})").mkString("\n")
        throw new IllegalStateException(
          s"Aucune ressource CSV pour '$slug'. Ressources disponibles :\n$available\n" +
            "Renseigner 'resource-id' (onglet Métadonnées) dans application.conf.")
      }
    }

  /** Écrit dans un fichier temporaire puis le renomme : jamais de fichier à moitié écrit en Bronze. */
  def ingest(dataset: DatasetConfig): Long = {
    val resourceId = resolveResourceId(dataset)
    val target: Path = Paths.get(config.bronzePath, dataset.file)
    Files.createDirectories(target.toAbsolutePath.getParent)
    val tmp = target.resolveSibling(target.getFileName.toString + ".part")

    log.info(s"Bronze : ressource $resourceId -> $target")
    val writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)
    var count = 0L
    try {
      tabular.foreachPage(resourceId) { page =>
        page.rows.foreach { row => writer.write(row); writer.newLine() }
        count += page.rows.size
        log.info(s"  ${dataset.name} : $count / ${page.total} lignes")
      }
    } catch {
      case e: Throwable =>
        writer.close()
        Files.deleteIfExists(tmp)
        throw e
    }
    writer.close()
    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING)
    count
  }

  def run(): Unit = config.datasets.foreach(ingest)
}

object BronzeJob {
  def apply(config: AppConfig): BronzeJob =
    new BronzeJob(
      config,
      new DataGouvClient(config.api.baseUrl),
      new TabularApiClient(config.api.tabularUrl, config.api.pageSize)
    )
}
