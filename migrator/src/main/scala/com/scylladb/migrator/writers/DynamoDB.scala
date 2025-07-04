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
import software.amazon.awssdk.services.dynamodb.model.{ BatchWriteItemRequest, DeleteRequest, WriteRequest }

object DynamoDB {

  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoDB")
  private val operationTypeColumnName = "_dynamo_op_type"
  private val MAX_BATCH_SIZE = 25 // DynamoDB limit for BatchWriteItem

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
        val dynamoClient = DynamoUtils.buildDynamoClient(
          target.endpoint,
          target.finalCredentials.map(_.toProvider),
          target.region
        )

        // Group items into batches
        partitionIterator.grouped(MAX_BATCH_SIZE).foreach { batch =>
          val writeRequests = new util.ArrayList[WriteRequest]()

          batch.foreach { itemV1 =>
            val itemKeyV2 = new util.HashMap[String, AttributeValueV2]()
            itemV1.asScala.foreach {
              case (key, valueV1) =>
                val targetKeyName = renamesMap.getOrElse(key, key)
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
              val deleteRequest = DeleteRequest.builder().key(itemKeyV2).build()
              writeRequests.add(WriteRequest.builder().deleteRequest(deleteRequest).build())
            }
          }

          if (!writeRequests.isEmpty) {
            val requestItems = new util.HashMap[String, util.List[WriteRequest]]()
            requestItems.put(target.table, writeRequests)

            var batchWriteRequest = BatchWriteItemRequest.builder().requestItems(requestItems).build()
            var unprocessedItems: util.Map[String, util.List[WriteRequest]] = null
            var attempts = 0
            val maxRetries = 5 // Configurable max retries

            do {
              try {
                attempts += 1
                log.info(s"Attempting to delete batch of ${writeRequests.size()} items from ${target.table}. Attempt #$attempts")
                val batchWriteResponse = dynamoClient.batchWriteItem(batchWriteRequest)
                unprocessedItems = batchWriteResponse.unprocessedItems()

                if (unprocessedItems != null && !unprocessedItems.isEmpty) {
                  log.warn(s"Batch delete resulted in ${unprocessedItems.get(target.table).size()} unprocessed items. Retrying...")
                  batchWriteRequest = BatchWriteItemRequest.builder().requestItems(unprocessedItems).build()
                  // Implement exponential backoff if needed
                  Thread.sleep(1000 * attempts) // Simple linear backoff
                } else {
                  log.info(s"Successfully deleted batch of ${writeRequests.size()} items from ${target.table}.")
                  unprocessedItems = null // Ensure loop termination
                }
              } catch {
                case e: Exception =>
                  log.error(
                    s"Failed to delete batch from ${target.table} on attempt #$attempts: ${e.getMessage}",
                    e)
                  if (attempts >= maxRetries) {
                    log.error(s"Exceeded max retries ($maxRetries) for batch delete. Giving up on this batch.")
                    unprocessedItems = null // Ensure loop termination and skip this batch
                  } else {
                    // Prepare for retry, unprocessedItems will be used from the request if available,
                    // otherwise, the whole batch is considered unprocessed for the next attempt.
                    // This part might need more sophisticated handling if partial success occurs within a batch that throws an exception.
                    // For simplicity, we retry the last `batchWriteRequest`.
                    Thread.sleep(1000 * attempts) // Simple linear backoff before retrying the same batch
                  }
              }
            } while (unprocessedItems != null && !unprocessedItems.isEmpty && attempts < maxRetries)

            if (unprocessedItems != null && !unprocessedItems.isEmpty) {
              log.error(s"Failed to delete ${unprocessedItems.get(target.table).size()} items from ${target.table} after $maxRetries retries. These items will be skipped.")
            }
          }
        }
        dynamoClient.close()
      }
    }
  }
}
