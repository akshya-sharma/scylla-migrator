package com.scylladb.migrator.writers

import com.scylladb.migrator.DynamoUtils
import com.scylladb.migrator.DynamoUtils.{ setDynamoDBJobConf, setOptionalConf }
import com.scylladb.migrator.config.TargetSettings
import org.apache.hadoop.dynamodb.{ DynamoDBConstants, DynamoDBItemWritable }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.LogManager
import com.scylladb.migrator.AttributeValueUtils // Required for V1 to V2 conversion
import com.scylladb.migrator.DynamoUtils
import com.scylladb.migrator.DynamoUtils.{ setDynamoDBJobConf, setOptionalConf }
import com.scylladb.migrator.config.TargetSettings
import org.apache.hadoop.dynamodb.{ DynamoDBConstants, DynamoDBItemWritable }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue => AttributeValueV2, // Alias for AWS SDK V2 AttributeValue
  DeleteItemRequest,
  TableDescription
}
import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue => AttributeValueV1
} // AWS SDK V1 AttributeValue

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
      val item = itemWritable.getItem // This is a java.util.Map[String, software.amazon.awssdk.services.dynamodb.model.AttributeValue] (SDK V2)

      // Remove the operationTypeColumnName attribute as it's not part of the actual table schema
      val filteredScalaMap = item.asScala.view.filterKeys(_ != operationTypeColumnName).toMap
      val modifiedItem = new java.util.HashMap[String, AttributeValueV2]()
      filteredScalaMap.foreach { case (k, v) => modifiedItem.put(k, v) }
      val newItemWritable = new DynamoDBItemWritable()
      newItemWritable.setItem(modifiedItem)
      newItemWritable
    }

    val finalRdd =
      if (renamesMap.isEmpty) processedRdd
      else
        processedRdd.mapValues { itemWritable =>
          val item = new util.HashMap[String, AttributeValueV2]()
          // Apply renames to keys
          itemWritable.getItem.forEach { (key, value) =>
            item.put(renamesMap.getOrElse(key, key), value)
          }
          itemWritable.setItem(item)
          itemWritable
        }
    finalRdd.saveAsHadoopDataset(jobConf)
  }

  def deleteRDD(target: TargetSettings.DynamoDB,
                renamesMap: Map[String, String], // For mapping key names if they were renamed
                rdd: RDD[java.util.Map[String, AttributeValueV1]], // Items from stream are V1
                targetTableDesc: TableDescription)(implicit spark: SparkSession): Unit = {

    val keySchema = targetTableDesc.keySchema().asScala.map(_.attributeName()).toSet
    log.info(s"Key schema for deletions on table ${target.table}: ${keySchema.mkString(", ")}")

    rdd.foreachPartition { partitionIterator =>
      if (partitionIterator.nonEmpty) {
        // Create a DynamoDB client for each partition.
        // This client will be used to delete items from ScyllaDB/Alternator.
        val dynamoClient = DynamoUtils.buildDynamoClient(
          target.endpoint,
          target.finalCredentials
            .map(_.toProvider), // Corrected: Apply .toProvider and pass as creds
          target.region // Corrected: Pass region as the third argument
        )

        partitionIterator.foreach { itemV1 =>
          // itemV1 is java.util.Map[String, AttributeValueV1]
          val itemKeyV2 = new util.HashMap[String, AttributeValueV2]()

          itemV1.asScala.foreach {
            case (key, valueV1) =>
              val targetKeyName = renamesMap.getOrElse(key, key) // Apply rename if exists
              if (keySchema.contains(targetKeyName)) {
                itemKeyV2.put(targetKeyName, AttributeValueUtils.fromV1(valueV1))
              }
          }

          if (itemKeyV2.isEmpty) {
            log.warn(s"Skipping delete for item as no key attributes found after mapping. Original item keys: ${itemV1
              .keySet()
              .asScala
              .mkString(", ")}. Renamed keys for schema: ${itemKeyV2.keySet().asScala.mkString(", ")}")
          } else if (itemKeyV2.size() != keySchema.size) {
            log.warn(s"Skipping delete for item due to mismatch in key attributes. Expected: ${keySchema.mkString(
              ", ")}, Found: ${itemKeyV2.keySet().asScala.mkString(", ")}. Original item: ${itemV1}")
          } else {
            try {
              val deleteRequest = DeleteItemRequest
                .builder()
                .tableName(target.table)
                .key(itemKeyV2)
                .build()

              dynamoClient.deleteItem(deleteRequest)
              log.debug(s"Deleted item with key ${itemKeyV2} from ${target.table}")
            } catch {
              case e: Exception =>
                log.error(
                  s"Failed to delete item with key ${itemKeyV2} from ${target.table}: ${e.getMessage}",
                  e)
            }
          }
        }
        dynamoClient.close() // Close client when partition processing is done
      }
    }
  }
}
