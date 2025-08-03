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
          val newMap = new util.HashMap[String, AttributeValueV1]()

          if (rec.getDynamodb.getNewImage ne null) {
            newMap.putAll(rec.getDynamodb.getNewImage)
          }

          newMap.putAll(rec.getDynamodb.getKeys)

          val operationType =
            rec.getEventName match {
              case "INSERT" | "MODIFY" => putOperation
              case "REMOVE"            => deleteOperation
            }
          newMap.put(operationTypeColumn, operationType)
          newMap.put(
            sequenceNumberColumn,
            new AttributeValueV1().withN(rec.getDynamodb.getSequenceNumber))
          Some(newMap)

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
    ).foreachRDD { msgs =>
      val rawRdd = msgs.collect { case Some(item) => item }

      if (rawRdd.isEmpty()) {
        log.info("No changes to apply in this batch.")
      } else {
        // To preserve the ordering of operations, we first de-duplicate the changes in the
        // micro-batch, keeping only the last change for each item, according to the
        // stream sequence number.
        //
        // Because the AWS SDK V1 data types are not serializable, we first need to convert
        // the items to our serializable DdbValue representation.
        val serializableRdd = rawRdd
          .map { itemV1 =>
            itemV1.asScala.map { case (k, v) => k -> DdbValue.from(AttributeValueUtils.fromV1(v)) }.toMap
          }
          .repartition(Runtime.getRuntime.availableProcessors() * 2)

        val keySchemaNames = targetTableDesc.keySchema.asScala.map(_.attributeName)

        // Group by primary key and find the latest operation for each key
        val latestOperationsRdd = serializableRdd
          .keyBy(item => keySchemaNames.map(k => item(k)).toList)
          .reduceByKey { (op1, op2) =>
            val seqNum1 = BigInt(op1(sequenceNumberColumn).asInstanceOf[DdbValue.N].value)
            val seqNum2 = BigInt(op2(sequenceNumberColumn).asInstanceOf[DdbValue.N].value)
            if (seqNum1 > seqNum2) op1 else op2
          }
          .values
          .cache()

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
