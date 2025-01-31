package com.labs1904.hwe

import com.labs1904.hwe.WordCountBatchApp.splitSentenceIntoWords
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}

import java.util.Properties

/**
 * Spark Structured Streaming app
 */
object WordCountStreamingApp {
	lazy val logger: Logger = Logger.getLogger(this.getClass)
	val jobName = "WordCountStreamingApp"
	// TODO: define the schema for parsing data from Kafka

	val bootstrapServer: String = "b-3-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196,b-2-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196,b-1-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196"
	val username: String = "1904labs"
	val password: String = "1904labs"
	val Topic: String = "word-count"

	//Use this for Windows
	val trustStore: String = "src\\main\\resources\\kafka.client.truststore.jks"
	//Use this for Mac
	//val trustStore: String = "src/main/resources/kafka.client.truststore.jks"

	def main(args: Array[String]): Unit = {
		logger.info(s"$jobName starting...")

		try {
			val spark = SparkSession.builder()
				.appName(jobName)
				.config("spark.sql.shuffle.partitions", "3")
				.master("local[*]")
				.getOrCreate()

			import spark.implicits._

			val sentences = spark
				.readStream
				.format("kafka")
				.option("maxOffsetsPerTrigger", 10)
				.option("kafka.bootstrap.servers", bootstrapServer)
				.option("subscribe", "word-count")
				.option("kafka.acks", "1")
				.option("kafka.key.serializer", classOf[StringSerializer].getName)
				.option("kafka.value.serializer", classOf[StringSerializer].getName)
				.option("startingOffsets", "earliest")
				.option("kafka.security.protocol", "SASL_SSL")
				.option("kafka.sasl.mechanism", "SCRAM-SHA-512")
				.option("kafka.ssl.truststore.location", trustStore)
				.option("kafka.sasl.jaas.config", getScramAuthString(username, password))
				.load()
				.selectExpr("CAST(value AS STRING)").as[String]

			sentences.printSchema

			val query = sentences
				.writeStream
				.outputMode(OutputMode.Append())
				.format("console")
				.trigger(Trigger.ProcessingTime("1 seconds"))
				.foreachBatch((batch: Dataset[String], _: Long) =>
					batch
						.flatMap(sentence => splitSentenceIntoWords(sentence))
						.groupBy($"value")
						.count()
						.sort(col("count").desc)
						.show(10)
				)
				.start()

			query.awaitTermination()
		} catch {
			case e: Exception => logger.error(s"$jobName error in main", e)
		}
	}

	def getScramAuthString(username: String, password: String) = {
		s"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username=\"$username\"
   password=\"$password\";"""
	}
}
