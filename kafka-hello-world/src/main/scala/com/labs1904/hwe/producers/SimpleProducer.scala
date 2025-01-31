package com.labs1904.hwe.producers

import com.labs1904.hwe.util.Connection._
import com.labs1904.hwe.util.Util
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object SimpleProducer {
  val Topic: String = "question-1-output"

  def main(args: Array[String]): Unit = {
    // Create Kafka Producer
    val properties = Util.getProperties(BOOTSTRAP_SERVER)
    val producer = new KafkaProducer[String, String](properties)
    val messageToSend = "Derek message"

    val record = new ProducerRecord[String, String](Topic, messageToSend)
    producer.send(record)
    producer.close()
  }


}
