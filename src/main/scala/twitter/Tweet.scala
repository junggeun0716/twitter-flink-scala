package twitter

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import java.io.IOException

class Tweet(val userName: String, val lang: String, val source: String, val text: String, val rawText: String) {
  override def toString: String = {
    this.userName + "; " + this.lang + "; " + this.source + "; " + this.text + "; "
  }
}

object Tweet {
  def fromString(s: String): Tweet = {
    val jsonParser = new ObjectMapper
    try {
      val node = jsonParser.readValue(s, classOf[JsonNode])
      val isLang = node.has("user") && node.has("lang")

      if (isLang && node.has("text")) {
        var source: String = "unknown"
        val userNode = node.get("user")

        if (node.has("source")) {
          val nodeSource = node.get("source").asText.toLowerCase
          if (nodeSource.contains("android")) source = "Android"
          else if (nodeSource.contains("iphone")) source = "iphone"
          else if (nodeSource.contains("web")) source = "web"
        }

        return new Tweet(userNode.get("name").asText, node.get("lang").asText, source,  node.get("text").asText, s)
      }
    } catch {
      case e: JsonProcessingException =>
        e.printStackTrace()
      case e: IOException =>
        e.printStackTrace()
    }
    null
  }
}

