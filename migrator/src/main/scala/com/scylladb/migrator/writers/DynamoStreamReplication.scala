package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1, Record }
import com.scylladb.migrator.AttributeValueUtils
import com.scylladb.migrator.config.{ AWSCredentials, SourceSettings, TargetSettings }
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kinesis.{
  KinesisDynamoDBInputDStream,
  KinesisInitialPositions,
  SparkAWSCredentials
}
import com.scylladb.migrator.alternator.DdbValue
import software.amazon.awssdk.services.dynamodb.model.TableDescription

import java.util
import scala.jdk.CollectionConverters._

object DynamoStreamReplication {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoStreamReplication")

  // We enrich the table items with a column `operationTypeColumn` describing the type of change
  // applied to the item.
  // We have to deal with multiple representation of the data because `spark-kinesis-dynamodb`
  // uses the AWS SDK V1, whereas `emr-dynamodb-hadoop` uses the AWS SDK V2
  private val operationTypeColumn = "_dynamo_op_type"
  private val sequenceNumberColumn = "_dynamo_sequence_number"
  private val putOperation = new AttributeValueV1().withBOOL(true)
  private val deleteOperation = new AttributeValueV1().withBOOL(false)

  def createDStream(spark: SparkSession,
                    streamingContext: StreamingContext,
                    src: SourceSettings.DynamoDB,
                    target: TargetSettings.DynamoDB,
                    targetTableDesc: TableDescription,
                    renamesMap: Map[String, String]): Unit =
    new KinesisDynamoDBInputDStream(
      streamingContext,
      streamName        = src.table,
      regionName        = src.region.orNull,
      initialPosition   = new KinesisInitialPositions.TrimHorizon,
      checkpointAppName = s"migrator_${src.table}_${System.currentTimeMillis()}",
      messageHandler = {
        case recAdapter: RecordAdapter =>
          val rec = recAdapter.getInternalObject
          val dynamoRecord = rec.getDynamodb

          val itemV1 = new util.HashMap[String, AttributeValueV1]

          val operationType = rec.getEventName match {
            case "INSERT" | "MODIFY" =>
              if (dynamoRecord.getNewImage ne null) {
                itemV1.putAll(dynamoRecord.getNewImage)
              }
              putOperation
            case "REMOVE" =>
              if (dynamoRecord.getKeys ne null) {
                itemV1.putAll(dynamoRecord.getKeys)
              }
              deleteOperation
          }

          if (itemV1.isEmpty) {
            None // Should not happen for valid records, but good to be safe
          } else {
            itemV1.put(operationTypeColumn, operationType)
            // need SequenceNumber for ordering events in stream
            itemV1.put(
              sequenceNumberColumn,
              new AttributeValueV1().withN(dynamoRecord.getSequenceNumber))

            // Convert to a serializable representation right away to avoid issues
            // with non-serializable ByteBuffers in the stream
            val ddbValueMap = itemV1.asScala.map {
              case (k, v) => k -> DdbValue.from(AttributeValueUtils.fromV1(v))
            }.toMap
            // Add log to print contents of the item ddbValueMap            l
            //log.info(s"Processing item from stream: ${ddbValueMap.mkString(", ")}")
            Some(ddbValueMap)
          }

        case _ => None
      },
      kinesisCreds = src.credentials
        .map {
          case AWSCredentials(accessKey, secretKey, maybeAssumeRole) =>
            val builder =
              SparkAWSCredentials.builder
                .basicCredentials(accessKey, secretKey)
            for (assumeRole <- maybeAssumeRole) {
              builder.stsCredentials(assumeRole.arn, assumeRole.getSessionName)
            }
            builder.build()
        }
        .getOrElse(SparkAWSCredentials.builder.build())
    ).foreachRDD { rdd =>
      //rdd.foreach(item => log.info(s"rdd RDD item: ${item.mkString(", ")}"))

      if (rdd.isEmpty()) {
        log.info("No changes to apply in this batch.")
      } else {
        // To preserve the ordering of operations, we first de-duplicate the changes in the
        // micro-batch, keeping only the last change for each item, according to the
        // stream sequence number.

        val keySchemaNames = targetTableDesc.keySchema.asScala.map(_.attributeName)

        // Group by primary key and find the latest operation for each key
        val latestOperationsRdd = rdd
          .flatMap(identity)
          .keyBy(item => keySchemaNames.map(k => item(k)))
          .reduceByKey { (op1, op2) =>
            val seqNum1 = BigInt(op1(sequenceNumberColumn).asInstanceOf[DdbValue.N].value)
            val seqNum2 = BigInt(op2(sequenceNumberColumn).asInstanceOf[DdbValue.N].value)
            if (seqNum1 > seqNum2) op1 else op2
          }
          .values
          .cache()

        // print contents of latestOperationsRdd
        // latestOperationsRdd.foreach(item =>
        //  log.info(s"latestOperationsRdd RDD item: ${item.mkString(", ")}"))

        val putOperationDdb = DdbValue.from(AttributeValueUtils.fromV1(putOperation))
        val deleteOperationDdb = DdbValue.from(AttributeValueUtils.fromV1(deleteOperation))

        val upsertRdd = latestOperationsRdd.filter { item =>
          item(operationTypeColumn) == putOperationDdb
        }

        val deleteRdd = latestOperationsRdd.filter { item =>
          item(operationTypeColumn) == deleteOperationDdb
        }

        val upsertCount = upsertRdd.count()
        val deleteCount = deleteRdd.count()

        //print contents of upsertRdd
        //upsertRdd.foreach(item => log.info(s"Upsert RDD item: ${item.mkString(", ")}"))

        if (upsertCount > 0) {
          log.info(s"Processing ${upsertCount} UPSERT operations.")
          // Convert DdbValue to V2 for writing via Hadoop EMR connector
          val writableUpsertRdd = upsertRdd.map { itemDdb =>
            val itemV2 = new util.HashMap[
              String,
              software.amazon.awssdk.services.dynamodb.model.AttributeValue]
            itemDdb
              .filterKeys(k => k != operationTypeColumn && k != sequenceNumberColumn)
              .foreach { case (key, ddbValue) => itemV2.put(key, DdbValue.toV2(ddbValue)) }
            (new Text, new DynamoDBItemWritable(itemV2))
          }

          // print contents of writableUpsertRdd
          //writableUpsertRdd.foreach(item =>
          //  log.info(s"Upserting item: ${item._2.getItem.asScala.mkString(", ")}"))

          DynamoDB.writeRDD(target, renamesMap, writableUpsertRdd, targetTableDesc)(spark)
        } else {
          log.info("No UPSERT operations in this batch.")
        }

        if (deleteCount > 0) {
          log.info(s"Processing ${deleteCount} DELETE operations.")
          // Convert DdbValue to V1 for deletion
          val deleteItemsV1Rdd: RDD[util.Map[String, AttributeValueV1]] =
            deleteRdd.map { itemDdb =>
              val itemV1 = new util.HashMap[String, AttributeValueV1]
              itemDdb.foreach { case (key, ddbValue) => itemV1.put(key, DdbValue.toV1(ddbValue)) }
              itemV1
            }
          DynamoDB.deleteRDD(target, renamesMap, deleteItemsV1Rdd, targetTableDesc)(spark)
        } else {
          log.info("No DELETE operations in this batch.")
        }

        latestOperationsRdd.unpersist()
      }
    }
}
