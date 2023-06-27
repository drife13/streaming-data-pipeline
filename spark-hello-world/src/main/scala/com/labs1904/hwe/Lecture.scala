package com.labs1904.hwe

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

// import spark.implicits._
case class EnrichedUser(ID: Int, Name: String, State: String, Email: String, Multiplied: Int)

object Lecture {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder()
			.appName("HWE Demo")
			.master("local[*]")
			.getOrCreate()

		val schema = new StructType()
			.add("ID", IntegerType, false)
			.add("Name", StringType, false)
			.add("State", StringType, false)
			.add("Email", StringType, false)

		val df = spark
			.read
			.schema(schema)
			.csv("src/main/resources/user.txt")

		df.printSchema()
		df.show(2)

		import spark.implicits._
		val times3 = df.map(row => EnrichedUser(row.getInt(0), row.getString(1), row.getString(2), row.getString(3), row.getInt(0) * 3))
		val filtered = times3.filter(col("Multiplied") < 7)
		val countState = filtered.groupBy(col("Multiplied")).count()
		countState.show(3)
	}
}
