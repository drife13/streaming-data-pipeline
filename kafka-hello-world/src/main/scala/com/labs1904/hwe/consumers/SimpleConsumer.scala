package com.labs1904.hwe.consumers

import com.labs1904.hwe.util.Connection._
import com.labs1904.hwe.util.Util
import net.liftweb.json.DefaultFormats
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Arrays

object SimpleConsumer {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val formats: DefaultFormats.type = DefaultFormats

  case class RawUser(id: Int, name: String, email: String)
  case class EnrichedUser(id: Int, name: String, email: String, numberAsWord: String, hweDeveloper: String)

  def main(args: Array[String]): Unit = {

    // Create the KafkaConsumer
    val properties = Util.getConsumerProperties(BOOTSTRAP_SERVER)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](properties)

    // Subscribe to the topic
    consumer.subscribe(Arrays.asList(DEFAULT_TOPIC))

    while ( {
      true
    }) {
      // poll for new data
      val duration: Duration = Duration.ofMillis(100)
      val records: ConsumerRecords[String, String] = consumer.poll(duration)

      records.forEach((record: ConsumerRecord[String, String]) => {
        // Retrieve the message from each record
        val message = record.value()

        val values = message.split(",")

        val rawUser = values match {
          case Array(f1, f2, f3) => RawUser(f1.toInt, f2, f3)
          case _ => throw new IllegalArgumentException("Invalid number of values")
        }

        val enrichedUser = EnrichedUser(rawUser.id, rawUser.name, rawUser.email, Util.numberToWordMap(rawUser.id), "Derek")

        logger.info(s"Message Received: $message")
      })
    }
  }
}
