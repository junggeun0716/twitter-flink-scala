package twitter

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.util.Collector

import java.io.FileInputStream
import java.util.Properties

case class Retweet(startWindow: Long, retweetId: Option[String], retweetText: Option[String], count: Int)

object StreamingJob {
  def main(args: Array[String]) {
    val config: Configuration = new Configuration
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Read twitter key from .env located in project_home
    val props: Properties = new Properties
    props.load(new FileInputStream(".env"))
    val tweetStream: DataStream[String] = env
      .addSource(new TwitterSource(props))
    val windowSize = Time.seconds(60)

    tweetStream
      .flatMap((r: String, out: Collector[Tweet]) => {
        val tweet: Tweet = Tweet.fromString(r)
        if (tweet != null) out.collect(tweet)
      })
      .assignTimestampsAndWatermarks(new TweetTimeAssigner)
      .filter(r => r.retweeted_status_id.isDefined)
      .map(r => (r.retweeted_status_id, r.retweeted_status_text, 1))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(windowSize))
      .reduce((r1: (Option[String], Option[String], Int), r2: (Option[String], Option[String], Int)) => {(r1._1, r1._2, r1._3 + 1)})
      .windowAll(TumblingEventTimeWindows.of(windowSize))
      .apply(new TopNRetweet(1))
      .print()

    // execute program
    env.execute("Flink Streaming Scala API With TwitterSource")
  }
}

class TweetTimeAssigner
  extends BoundedOutOfOrdernessTimestampExtractor[Tweet](Time.seconds(5)) {
  override def extractTimestamp(r: Tweet): Long = r.timestamp
}

class TopNRetweet(n: Int)
  extends AllWindowFunction[(Option[String], Option[String], Int), Retweet, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[(Option[String], Option[String], Int)], out: Collector[Retweet]): Unit = {
    val windowStart = window.getStart
    val topN = input.toSeq.sortBy(-_._3).take(n).map(r => Retweet(windowStart, r._1, r._2, r._3))
    topN.foreach(r => out.collect(r))
  }
}