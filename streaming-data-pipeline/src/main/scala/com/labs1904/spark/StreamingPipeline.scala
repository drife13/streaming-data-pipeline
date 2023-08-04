package com.labs1904.spark

import com.labs1904.spark.Util.{constructSchema, schemaValidationFilter}
import com.labs1904.spark.classes.{Review, User, UserReview}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_csv
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.sql.Date


/**
 * Spark Structured Streaming app
 *
 */
object StreamingPipeline {
	lazy val logger: Logger = Logger.getLogger(this.getClass)

	implicit def stringToBytes(s: String): Array[Byte] = Bytes.toBytes(s)

	implicit def bytesToString(b: Array[Byte]): String = Bytes.toString(b)

	implicit def bytesToDate(b: Array[Byte]): Date = Date.valueOf(Bytes.toString(b))

	val jobName = "StreamingPipeline"

	val hbaseUrl = "hbase01.labs1904.com:2181"
	val hdfsUrl = "hdfs://manager.hourswith.expert:8020"
	val bootstrapServers = System.getenv("HWE_KAFKA_BOOTSTRAP_SERVER")
	val username = System.getenv("HWE_KAFKA_USERNAME")
	val password = System.getenv("HWE_KAFKA_PASSWORD")
	val hdfsUsername = "khull"

	//Use this for Windows
	val trustStore: String = "src\\main\\resources\\kafka.client.truststore.jks"

	def main(args: Array[String]): Unit = {
		try {
			val spark = SparkSession.builder()
				.appName(jobName)
				.config("spark.sql.shuffle.partitions", "3")
				.config("spark.sql.legacy.timeParserPolicy", "LEGACY")
				.master("local[*]")
				.getOrCreate()

			//      val spark = SparkSession.builder(
			//        .config("spark.sql.shuffle.partitions", "3")
			//        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
			//        .config("spark.hadoop.fs.defaultFS", hdfsUrl)
			//        .appName(jobName)
			//        .master("local[*]")
			//        .getOrCreate()

			val reviewSchema = constructSchema[Review]

			import spark.implicits._
			val ds = spark
				.readStream
				.format("kafka")
				.option("kafka.bootstrap.servers", bootstrapServers)
				.option("subscribe", "reviews")
				.option("startingOffsets", "earliest")
				.option("maxOffsetsPerTrigger", 20)
				.option("startingOffsets", "earliest")
				.option("kafka.security.protocol", "SASL_SSL")
				.option("kafka.sasl.mechanism", "SCRAM-SHA-512")
				.option("kafka.ssl.truststore.location", trustStore)
				.option("kafka.sasl.jaas.config", getScramAuthString(username, password))
				.load()
				.selectExpr("CAST(value AS STRING)")
				.select(from_csv($"value", reviewSchema, Map("sep" -> "\t")).alias("r"))
				.select($"r.*")
				.filter(schemaValidationFilter(reviewSchema))
				.as[Review]

			//      val ds = spark
			//        .read
			//        .option("delimiter", "\t")
			//        .schema(reviewSchema)
			//        .csv("src/main/resources/reviews-and-junk.tsv")
			//        .filter(schemaValidationFilter(reviewSchema))
			//        .as[Review]
			//      ds.show()

			val result = ds.mapPartitions(p => {
				val conf = HBaseConfiguration.create()
				conf.set("hbase.zookeeper.quorum", hbaseUrl)
				val connection = ConnectionFactory.createConnection(conf)
				val table = connection.getTable(TableName.valueOf("kmenke:users"))

				val iter = p.map(review => {
					val get = new Get(review.customer_id.toString).addFamily("f1")
					val result = table.get(get)
					val user = User(
						result.getValue("f1", "mail"),
						result.getValue("f1", "birthdate"),
						result.getValue("f1", "name"),
						result.getValue("f1", "sex"),
						result.getValue("f1", "username"),
					)

					UserReview(user, review)
				}).toList.iterator

				connection.close()

				iter
			})

			// Write output to console
			val query = result
				.toJSON
				.writeStream
				.outputMode(OutputMode.Append())
				.format("console")
				.option("truncate", false)
				.trigger(Trigger.ProcessingTime("5 seconds"))
				.start()

			// Write output to HDFS
			//      val query = result.writeStream
			//        .outputMode(OutputMode.Append())
			//        .format("json")
			//        .option("path", s"/user/${hdfsUsername}/reviews_json")
			//        .option("checkpointLocation", s"/user/${hdfsUsername}/reviews_checkpoint")
			//        .trigger(Trigger.ProcessingTime("5 seconds"))
			//        .start()

			//      val query = result.writeStream
			//        .outputMode(OutputMode.Append())
			//        .format("json")
			//        .option("path", "hdfs://manager.hourswith.expert:8020/user/wfarrell/reviews")
			//        .option("checkpointLocation", "hdfs://manager.hourswith.expert:8020/user/wfarrell/reviews_checkpoint")
			//        .trigger(Trigger.ProcessingTime("5 seconds"))
			//        .start()

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
