package com.scylladb.migrator.validation

import com.datastax.spark.connector.CassandraRow
import com.google.common.math.DoubleMath
import com.scylladb.migrator.alternator.DdbValue

import java.time.temporal.ChronoUnit

case class RowComparisonFailure(rowRepr: String,
                                otherRepr: Option[String],
                                items: List[RowComparisonFailure.Item]) {

  override def toString: String =
    s"""
       |Row failure:
       |* Source row: ${rowRepr}
       |* Target row: ${otherRepr.getOrElse("<MISSING>")}
       |* Failures:
       |${items.map(item => s"  - ${item.description}").mkString("\n")}
     """.stripMargin
}

object RowComparisonFailure {
  sealed abstract class Item(val description: String) extends Serializable
  object Item {
    case object MissingTargetRow extends Item("Missing target row")
    case object MismatchedColumnCount extends Item("Mismatched column count")
    case object MismatchedColumnNames extends Item("Mismatched column names")
    case class DifferingFieldValues(fields: List[String])
        extends Item(s"Differing fields: ${fields.mkString(", ")}")
    case class DifferingTtls(details: List[(String, Long)])
        extends Item(s"Differing TTLs: ${details
          .map {
            case (fieldName, ttlDiff) => s"$fieldName ($ttlDiff millis)"
          }
          .mkString(", ")}")
    case class DifferingWritetimes(details: List[(String, Long)])
        extends Item(s"Differing WRITETIMEs: ${details
          .map {
            case (fieldName, writeTimeDiff) => s"$fieldName ($writeTimeDiff millis)"
          }
          .mkString(", ")}")
  }

  def cassandraRowComparisonFailure(left: CassandraRow,
                                    right: Option[CassandraRow],
                                    items: List[Item]): RowComparisonFailure =
    RowComparisonFailure(left.toString, right.map(_.toString), items)

  def compareCassandraRows(left: CassandraRow,
                           right: Option[CassandraRow],
                           floatingPointTolerance: Double,
                           timestampMsTolerance: Long,
                           ttlToleranceMillis: Long,
                           writetimeToleranceMillis: Long,
                           compareTimestamps: Boolean): Option[RowComparisonFailure] =
    right match {
      case None => Some(cassandraRowComparisonFailure(left, right, List(Item.MissingTargetRow)))
      case Some(right) if left.columnValues.size != right.columnValues.size =>
        Some(cassandraRowComparisonFailure(left, Some(right), List(Item.MismatchedColumnCount)))
      case Some(right) if left.metaData.columnNames != right.metaData.columnNames =>
        Some(cassandraRowComparisonFailure(left, Some(right), List(Item.MismatchedColumnNames)))
      case Some(right) =>
        val names = left.metaData.columnNames

        val leftMap = left.toMap
        val rightMap = right.toMap

        val differingFieldValues =
          for {
            name <- names
            if !name.endsWith("_ttl") && !name.endsWith("_writetime")
            leftValue  = leftMap.get(name)
            rightValue = rightMap.get(name)
            if areDifferent(leftValue, rightValue, timestampMsTolerance, floatingPointTolerance)
          } yield name

        val differingTtls =
          if (!compareTimestamps) Nil
          else
            for {
              name <- names
              if name.endsWith("_ttl")
              leftTtl  = left.getLongOption(name)
              rightTtl = right.getLongOption(name)
              result <- (leftTtl, rightTtl) match {
                         case (Some(l), Some(r)) if math.abs(l - r) > ttlToleranceMillis =>
                           Some(name -> math.abs(l - r))
                         case (Some(l), None)    => Some(name -> l)
                         case (None, Some(r))    => Some(name -> r)
                         case (Some(l), Some(r)) => None
                         case (None, None)       => None
                       }
            } yield result

        // WRITETIME is expressed in microseconds
        val writetimeToleranceMicros = writetimeToleranceMillis * 1000
        val differingWritetimes =
          if (!compareTimestamps) Nil
          else
            for {
              name <- names
              if name.endsWith("_writetime")
              leftWritetime  = left.getLongOption(name)
              rightWritetime = right.getLongOption(name)
              result <- (leftWritetime, rightWritetime) match {
                         case (Some(l), Some(r)) if math.abs(l - r) > writetimeToleranceMicros =>
                           Some(name -> math.abs(l - r))
                         case (Some(l), None)    => Some(name -> l)
                         case (None, Some(r))    => Some(name -> r)
                         case (Some(l), Some(r)) => None
                         case (None, None)       => None
                       }
            } yield result

        if (differingFieldValues.isEmpty && differingTtls.isEmpty && differingWritetimes.isEmpty)
          None
        else
          Some(
            cassandraRowComparisonFailure(
              left,
              Some(right),
              (if (differingFieldValues.nonEmpty)
                 List(Item.DifferingFieldValues(differingFieldValues.toList))
               else Nil) ++
                (if (differingTtls.nonEmpty) List(Item.DifferingTtls(differingTtls.toList))
                 else Nil) ++
                (if (differingWritetimes.nonEmpty)
                   List(Item.DifferingWritetimes(differingWritetimes.toList))
                 else Nil)
            )
          )
    }

  def dynamoDBRowComparisonFailure(left: collection.Map[String, DdbValue],
                                   maybeRight: Option[collection.Map[String, DdbValue]],
                                   items: List[Item]): RowComparisonFailure =
    RowComparisonFailure(left.toString, maybeRight.map(_.toString), items)

  /**
    * @param left                   The first item to compare
    * @param maybeRight             The possible second item to compare
    * @param renamedColumn          A function describing how the columns of the first items should be expected to be
    *                               renamed in the second item
    * @param floatingPointTolerance The tolerance to apply when comparing floating point values
    * @return Some comparison failure if the compared items were different, otherwise `None`.
    */
  def compareDynamoDBRows(left: collection.Map[String, DdbValue],
                          maybeRight: Option[collection.Map[String, DdbValue]],
                          renamedColumn: String => String,
                          floatingPointTolerance: Double): Option[RowComparisonFailure] =
    maybeRight match {
      case None => Some(dynamoDBRowComparisonFailure(left, maybeRight, List(Item.MissingTargetRow)))
      case Some(right) if left.keySet.size != right.keySet.size =>
        Some(dynamoDBRowComparisonFailure(left, maybeRight, List(Item.MismatchedColumnCount)))
      case Some(right) if left.keySet.map(renamedColumn) != right.keySet =>
        Some(dynamoDBRowComparisonFailure(left, maybeRight, List(Item.MismatchedColumnNames)))
      case Some(right) =>
        val differingFieldValues =
          for {
            (columnName, leftValue) <- left
            rightValue = right.get(renamedColumn(columnName))
            if areDifferent(
              Some(leftValue),
              rightValue,
              0L, // There is no Timestamp type in DynamoDB
              floatingPointTolerance)
          } yield columnName
        if (differingFieldValues.isEmpty) None
        else
          Some(
            dynamoDBRowComparisonFailure(
              left,
              maybeRight,
              List(Item.DifferingFieldValues(differingFieldValues.toList))))
    }

  /**
    * @param leftValue              First value to compare
    * @param rightValue             Second value to compare
    * @param timestampMsTolerance   Tolerance when comparing instant values
    * @param floatingPointTolerance Tolerance when comparing floating point values
    * @return `true` if the `leftValue` is different from the `rightValue`
    */
  private def areDifferent(leftValue: Option[Any],
                           rightValue: Option[Any],
                           timestampMsTolerance: Long,
                           floatingPointTolerance: Double): Boolean =
    (rightValue, leftValue) match {
      // All timestamp types need to be compared with a configured tolerance
      case (Some(l: java.time.Instant), Some(r: java.time.Instant)) if timestampMsTolerance > 0 =>
        Math.abs(r.until(l, ChronoUnit.MILLIS)) > timestampMsTolerance
      // All floating-point-like types need to be compared with a configured tolerance
      case (Some(l: Float), Some(r: Float)) =>
        !DoubleMath.fuzzyEquals(l, r, floatingPointTolerance)
      case (Some(l: Double), Some(r: Double)) =>
        !DoubleMath.fuzzyEquals(l, r, floatingPointTolerance)
      case (Some(l: java.math.BigDecimal), Some(r: java.math.BigDecimal)) =>
        areNumericalValuesDifferent(l, r, floatingPointTolerance)

      // CQL blobs get converted to byte buffers by the Java driver, and the
      // byte buffers are converted to byte arrays by the Spark connector.
      // Arrays can't be compared with standard equality and must be compared
      // with `sameElements`.
      case (Some(l: Array[_]), Some(r: Array[_])) =>
        !l.sameElements(r)

      // Special cases for DynamoDB item values
      // B and Bs are case classes wrapping Array[Byte] and Seq[Array[Byte]].
      // Arrays must be compared for content equality.
      case (Some(DdbValue.B(l)), Some(DdbValue.B(r))) => !l.sameElements(r)
      case (Some(DdbValue.Bs(l)), Some(DdbValue.Bs(r))) =>
        l.size != r.size || l.zip(r).exists {
          case (lArray, rArray) => !lArray.sameElements(rArray)
        }
      case (Some(DdbValue.N(l)), Some(DdbValue.N(r))) =>
        areNumericalValuesDifferent(BigDecimal(l), BigDecimal(r), floatingPointTolerance)
      case (Some(DdbValue.Ns(l)), Some(DdbValue.Ns(r))) =>
        val xs = l.map(BigDecimal(_))
        val ys = r.map(BigDecimal(_))
        xs.size != ys.size || xs.zip(ys).exists {
          case (x, y) => areNumericalValuesDifferent(x, y, floatingPointTolerance)
        }

      // All remaining types get compared with standard equality
      case (Some(l), Some(r)) => l != r
      case (Some(_), None)    => true
      case (None, Some(_))    => true
      case (None, None)       => false
    }

  /** @return true iff the difference between `x` and `y` is greater than the `tolerance` */
  private def areNumericalValuesDifferent(x: BigDecimal,
                                          y: BigDecimal,
                                          tolerance: BigDecimal): Boolean =
    (x - y).abs > tolerance

}
