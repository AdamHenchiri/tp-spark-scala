package fr.valeuriad.data.bronze

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

/** GET HTTP renvoyant le corps en texte, avec quelques nouvelles tentatives si l'API sature (429) ou plante (5xx). */
class Http(maxRetries: Int = 3) {

  private val client = HttpClient.newBuilder()
    .followRedirects(HttpClient.Redirect.NORMAL)
    .connectTimeout(Duration.ofSeconds(30))
    .build()

  def get(url: String): String = {
    val request = HttpRequest.newBuilder(URI.create(url))
      .header("User-Agent", "tp-spark-job")
      .header("Accept", "application/json")
      .timeout(Duration.ofMinutes(2))
      .GET()
      .build()

    def attempt(n: Int): String = {
      val response = client.send(request, HttpResponse.BodyHandlers.ofString())
      val status = response.statusCode()
      if (status == 200) response.body()
      else if ((status == 429 || status >= 500) && n < maxRetries) {
        Thread.sleep(1000L * (n + 1))
        attempt(n + 1)
      } else throw new IllegalStateException(s"HTTP $status pour $url")
    }

    attempt(0)
  }
}
