import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SSKT_Consumer {
  def main(args: Array[String]): Unit = {

//    val conf = ConfigFactory.load()

    val sparkConf = new SparkConf().setAppName("SSKT")
    val sc = new StreamingContext(sparkConf, Seconds(5))

    val tweetStream = {
      val topics = Map(SSKT_Producer.KafkaTopic -> 1)
      val kafkaParams = Map(
        "metadata.broker.list" -> "localhost:9092",
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "1",
        "zookeeper.connection.timeout.ms" -> "1000")
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](sc, kafkaParams, topics, StorageLevel.MEMORY_ONLY)
    }

    val hashtags = tweetStream.map((x: (String, String)) => x._2)

    val wordCounts = hashtags.flatMap(_.split(" ")
      .map((_, 1))).reduceByKey(_ + _).filter(_._1 != "")
    val sortedCounts = wordCounts.transform(_.sortBy(_._2, ascending = false))

    sortedCounts.print()

    sc.start()
    sc.awaitTermination()
  }
}
