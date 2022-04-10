package twitter

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper

import java.io.IOException
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

case class Tweet(
             timestamp: Long,
             id_str: String,
             userName: String,
             lang: String,
             source: String,
             retweeted_status_id: Option[String],
             retweeted_status_text: Option[String],
             text: String
           )

object Tweet {
  def fromString(s: String): Tweet = {
    val jsonParser = new ObjectMapper
    try {
      val node = jsonParser.readValue(s, classOf[JsonNode])
      val isLang = node.has("user") && node.has("lang")

      if(isLang && node.has("text") && node.has("created_at")) {
        val created_at = node.get("created_at").asText
        val formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss XX uuuu").withLocale(Locale.ENGLISH)
        val timestamp = ZonedDateTime.parse(created_at, formatter).toEpochSecond * 1000

        var source: String = "unknown"
        val userNode = node.get("user")

        if(node.has("source")) {
          val nodeSource = node.get("source").asText.toLowerCase
          if(nodeSource.contains("android")) source = "Android"
          else if (nodeSource.contains("iphone")) source = "iphone"
          else if (nodeSource.contains("web")) source = "web"
        }

        var retweeted_status_id: Option[String] = None
        var retweeted_status_text: Option[String] = None
        if(node.has("retweeted_status")) {
          val retweeted_status = node.get("retweeted_status")
          retweeted_status_id = Some(retweeted_status.get("id_str").asText())
          retweeted_status_text = Some(retweeted_status.get("text").asText())
        }

        return new Tweet(
          timestamp,
          node.get("id_str").asText,
          userNode.get("name").asText,
          node.get("lang").asText,
          source,
          retweeted_status_id,
          retweeted_status_text,
          node.get("text").asText
        )
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

