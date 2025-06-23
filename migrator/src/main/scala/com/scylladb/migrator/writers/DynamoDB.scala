package com.scylladb.migrator.writers

import com.scylladb.migrator.DynamoUtils
import com.scylladb.migrator.DynamoUtils.{ setDynamoDBJobConf, setOptionalConf }
import com.scylladb.migrator.config.TargetSettings
import org.apache.hadoop.dynamodb.{ DynamoDBConstants, DynamoDBItemWritable }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, TableDescription }

import java.util

import scala.jdk.CollectionConverters._

object DynamoDB {

  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoDB")
  private val operationTypeColumnName = "_dynamo_op_type"

  def writeRDD(target: TargetSettings.DynamoDB,
               renamesMap: Map[String, String],
               rdd: RDD[(Text, DynamoDBItemWritable)],
               targetTableDesc: TableDescription)(implicit spark: SparkSession): Unit = {

    val jobConf = new JobConf(spark.sparkContext.hadoopConfiguration)

    setDynamoDBJobConf(
      jobConf,
      target.region,
      target.endpoint,
      maybeScanSegments = None,
      maybeMaxMapTasks  = None,
      target.finalCredentials)
    jobConf.set(DynamoDBConstants.OUTPUT_TABLE_NAME, target.table)
    val writeThroughput =
      target.writeThroughput match {
        case Some(value) =>
          log.info(
            s"Setting up Hadoop job to write table using a configured throughput of ${value} WCU(s)")
          value
        case None =>
          val value = DynamoUtils.tableWriteThroughput(targetTableDesc)
          log.info(
            s"Setting up Hadoop job to write table using a calculated throughput of ${value} WCU(s)")
          value
      }
    jobConf.set(DynamoDBConstants.WRITE_THROUGHPUT, writeThroughput.toString)
    setOptionalConf(
      jobConf,
      DynamoDBConstants.THROUGHPUT_WRITE_PERCENT,
      target.throughputWritePercent.map(_.toString))

    // Map over the RDD to potentially modify items
    val processedRdd = rdd.mapValues { itemWritable =>
      val item = itemWritable.getItem // This is a java.util.Map[String, AttributeValue]

      // Check if this is a PUT operation
      //if (item.get(operationTypeColumnName) == putOperationAttributeValue) {
      // If it's a PUT operation, remove the operationTypeColumnName attribute
      val filteredScalaMap = item.asScala.view.filterKeys(_ != operationTypeColumnName).toMap
      val modifiedItem = new java.util.HashMap[String, AttributeValue]()
      filteredScalaMap.foreach { case (k, v) => modifiedItem.put(k, v) }
      val newItemWritable = new DynamoDBItemWritable()
      newItemWritable.setItem(modifiedItem)
      newItemWritable // Return the modified itemWritable
    //} else {
    // If it's not a PUT operation (e.g., it's a DELETE or unknown), leave it as is
    //  itemWritable
    //}
    }

    val finalRdd =
      if (renamesMap.isEmpty) processedRdd // Use processedRdd here
      else
        processedRdd.mapValues { itemWritable =>
          val item = new util.HashMap[String, AttributeValue]()
          itemWritable.getItem.forEach((key, value) => item.put(renamesMap(key), value))
          itemWritable.setItem(item)
          itemWritable
        }
    finalRdd.saveAsHadoopDataset(jobConf)
  }
}
