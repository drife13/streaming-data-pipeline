package com.labs1904.hwe

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

object WordCountBatchApp {
	lazy val logger: Logger = Logger.getLogger(this.getClass)
	val jobName = "WordCountBatchApp"

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
				.read
				.csv("src/main/resources/sentences.txt").as[String]
			sentences.printSchema

			// TODO: implement me

			val counts = sentences
				.flatMap(sentence => splitSentenceIntoWords(sentence))
				.groupBy(col("value"))
				.count()
				.sort(col("count").desc)

			counts.show()
		} catch {
			case e: Exception => logger.error(s"$jobName error in main", e)
		}
	}

	// TODO: implement this function
	// HINT: you may have done this before in Scala practice...
	def splitSentenceIntoWords(sentence: String): Array[String] = {
		sentence
			.split("\\W+")
			.map((word: String) => word.toLowerCase().replaceAll("/[^a-z0-9]/", ""))
			.filter((word: String) => word.length != 0)
	}

}
