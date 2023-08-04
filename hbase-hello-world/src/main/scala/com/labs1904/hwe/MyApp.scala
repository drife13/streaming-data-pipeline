package com.labs1904.hwe

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.{LogManager, Logger}
import com.labs1904.hwe.util.HBaseConnection._
import org.apache.hadoop.hbase.filter.{BinaryComparator, CompareFilter, FilterList, RowFilter, SingleColumnValueFilter}


object MyApp {
  lazy val logger: Logger = LogManager.getLogger(this.getClass)
  implicit def stringToBytes(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def bytesToString(b: Array[Byte]): String = Bytes.toString(b)
  // implicit def arrayBytesToArrayStrings(b: Array[Array[Byte]]): Array[String] = b.map(bytesToString(_))

  def main(args: Array[String]): Unit = {
    logger.info("MyApp starting...")
    var connection: Connection = null
    try {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "hbase01.labs1904.com:2181")
      connection = ConnectionFactory.createConnection(conf)
      // Example code... change me
      val table = connection.getTable(TableName.valueOf("kmenke:users"))

      // Question 1
/*      var get = new Get("10000001")
      get.addColumn("f1", "mail")
      var result = table.get(get)
      val emailAddress: String = result.getValue("f1", "mail")
      logger.debug(emailAddress)*/

      // Question 2; birthdate,mail,name,sex,username
/*      var put = new Put("99")
      put.addColumn("f1", "username", "DE-HWE")
      put.addColumn("f1", "name", "The Panther")
      put.addColumn("f1", "sex", "F")
      put.addColumn("f1", "favorite_color", "pink")
      table.put(put)

      get = new Get("99")
      result = table.get(get)
      logger.debug(result)*/

      // Question 3
/*      val scan = new Scan("10000001", "10006002")
      val scanner = table.getScanner(scan)
      val count = Iterator.from(0).takeWhile(_ => scanner.next() != null).length
      scanner.close()

      logger.debug("Row count: " + count)*/

      // Question 4
      //
      val ids = List("9005729", "500600", "30059640", "6005263", "800182")
      val gets = ids.map(new Get(_))
      gets.foreach(_.addColumn("f1", "mail"))

      import scala.collection.JavaConverters._
      val results = table.get(gets.asJava)
      val emailAddresses = results.map(r => Bytes.toString(r.getValue("f1", "mail")))
      emailAddresses.foreach(logger.debug(_))

    } catch {
      case e: Exception => logger.error("Error in main", e)
    } finally {
      if (connection != null) connection.close()
    }
  }
}
