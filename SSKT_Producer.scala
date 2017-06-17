import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import twitter4j._
import twitter4j.conf.{Configuration, ConfigurationBuilder}
import com.twitter.bijection.avro.SpecificAvroCodecs.toJson
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object SSKT_Producer {

//  val conf = ConfigFactory.load()

  val KafkaTopic = "tweets"

  val queue = new LinkedBlockingQueue[Status](1000)

  val producer = {
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

  def main(args: Array[String]): Unit = {

    val cb = new ConfigurationBuilder()

    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(
        "eIf6vQHc8lJd2gI9DTKSkVV0H"
        //        conf.getString("twitter.consumerKey")
      )
      .setOAuthConsumerSecret(
        "Pbxnu74fUZbrBVVj0PBTtuLOa84gkNjsVxms5l3u4pwN3pVjZQ"
        //        conf.getString("twitter.consumerSecret")
      )
      .setOAuthAccessToken(
        "4873168809-2LWz0EhYrmkRSFU7UvBZyNd1HcPTqrB06XIxaQ9"
        //        conf.getString("twitter.accessToken")
      )
      .setOAuthAccessTokenSecret(
        "X9SyXXvLFGSHSZhcgHbRoXGooVB3kOSb6IodiLxmFPfWQ"
        //        conf.getString("twitter.accessTokenSecret")
      )

    val twitterStream = new TwitterStreamFactory(cb.build()).getInstance()

    val listener = new StatusListener {

      override def onStallWarning(warning: StallWarning) = {
        println("Got stall warning" + warning)
      }

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) = {
        println("Got status deletion notice" + statusDeletionNotice)
      }

      override def onScrubGeo(userId: Long, upToStatusId: Long) = {
        println("Got scrub_geo event userID:" + userId + "upToStatusID:" + upToStatusId)
      }

      override def onStatus(status: Status) = queue.offer(status)

      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int) = {
        println("Got track limitation notice:" + numberOfLimitedStatuses)
      }

      override def onException(ex: Exception) = {
        ex.printStackTrace()}
    }

    twitterStream.addListener(listener)

    val query = new FilterQuery().track(Array("US"))
    twitterStream.filter(query)

    while(true) {
      val ret = queue.poll()
      if (ret == null) {
        Thread.sleep(100);
      }
      else {
        val hashtags = ret.getHashtagEntities.map(_.getText)
        val msg = new ProducerRecord[String, String](KafkaTopic, hashtags.mkString(" "))
        producer.send(msg)
      }
    }
  }
}