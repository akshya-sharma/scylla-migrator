package com.scylladb.migrator.alternator

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.jdk.CollectionConverters._

/**
  * A data type isomorphic to AWS’ `AttributeValue`, but effectively serializable.
  * See https://github.com/aws/aws-sdk-java-v2/issues/3143
  * The fact that `AttributeValue` is not serializable anymore prevents us to use
  * it in RDD operations that may perform shuffles.
  * As a workaround, we convert values of type `AttributeValue` to `DdbValue`.
  */
@SerialVersionUID(1L)
sealed trait DdbValue extends Serializable

object DdbValue {
  case class S(value: String) extends DdbValue
  case class N(value: String) extends DdbValue
  case class Bool(value: Boolean) extends DdbValue
  case class L(values: collection.Seq[DdbValue]) extends DdbValue
  case class Null(value: Boolean) extends DdbValue
  // Getting serialization issue with SdkBytes
  //case class B(value: SdkBytes) extends DdbValue
  // using the primitive Array[Byte] which is guaranteed to be serialized correctly by Spark
  case class B(value: Array[Byte]) extends DdbValue
  case class M(value: Map[String, DdbValue]) extends DdbValue
  case class Ss(values: collection.Seq[String]) extends DdbValue
  case class Ns(values: collection.Seq[String]) extends DdbValue
  //case class Bs(values: collection.Seq[SdkBytes]) extends DdbValue
  case class Bs(values: collection.Seq[Array[Byte]]) extends DdbValue

  def from(value: AttributeValue): DdbValue =
    if (value.s() != null) S(value.s())
    else if (value.n() != null) N(value.n())
    else if (value.bool() != null) Bool(value.bool())
    else if (value.hasL) L(value.l().asScala.map(from))
    else if (value.nul() != null) Null(value.nul())
    //else if (value.b() != null) B(value.b())
    else if (value.b() != null) B(value.b().asByteArray())
    else if (value.hasM) M(value.m().asScala.view.mapValues(from).toMap)
    else if (value.hasSs) Ss(value.ss().asScala)
    else if (value.hasNs) Ns(value.ns().asScala)
    //else if (value.hasBs) Bs(value.bs().asScala)
    else if (value.hasBs) Bs(value.bs().asScala.map(_.asByteArray()))
    else sys.error("Unknown AttributeValue type")

  def toV2(value: DdbValue): AttributeValue = {
    val builder = AttributeValue.builder()
    value match {
      case S(v)    => builder.s(v)
      case N(v)    => builder.n(v)
      case Bool(v) => builder.bool(v)
      case L(v)    => builder.l(v.map(toV2).asJava)
      case Null(v) => builder.nul(v)
      //case B(v)    => builder.b(software.amazon.awssdk.core.SdkBytes.fromByteArray(v.asByteArray()))
      case B(v)  => builder.b(SdkBytes.fromByteArray(v))
      case M(v)  => builder.m(v.view.mapValues(toV2).toMap.asJava)
      case Ss(v) => builder.ss(v.asJava)
      case Ns(v) => builder.ns(v.asJava)
      //case Bs(v)   => builder.bs(v.asJava)
      case Bs(v) => builder.bs(v.map(SdkBytes.fromByteArray).asJava)
    }
    builder.build()
  }

  def toV1(value: DdbValue): com.amazonaws.services.dynamodbv2.model.AttributeValue =
    value match {
      case S(v)    => new com.amazonaws.services.dynamodbv2.model.AttributeValue().withS(v)
      case N(v)    => new com.amazonaws.services.dynamodbv2.model.AttributeValue().withN(v)
      case Bool(v) => new com.amazonaws.services.dynamodbv2.model.AttributeValue().withBOOL(v)
      case L(v) =>
        new com.amazonaws.services.dynamodbv2.model.AttributeValue().withL(v.map(toV1).asJava)
      case Null(v) => new com.amazonaws.services.dynamodbv2.model.AttributeValue().withNULL(v)
      case B(v) =>
        new com.amazonaws.services.dynamodbv2.model.AttributeValue()
          .withB(java.nio.ByteBuffer.wrap(v))
      case M(v) =>
        new com.amazonaws.services.dynamodbv2.model.AttributeValue()
          .withM(v.view.mapValues(toV1).toMap.asJava)
      case Ss(v) => new com.amazonaws.services.dynamodbv2.model.AttributeValue().withSS(v.asJava)
      case Ns(v) => new com.amazonaws.services.dynamodbv2.model.AttributeValue().withNS(v.asJava)
      case Bs(v) =>
        new com.amazonaws.services.dynamodbv2.model.AttributeValue()
          .withBS(v.map(java.nio.ByteBuffer.wrap).asJava)
    }
}
