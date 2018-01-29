package com.chrisbenincasa.redis.protocol

import java.nio.ByteBuffer
import java.nio.charset.Charset

object RedisResponse {
  val UTF8 = Charset.forName("UTF-8")
  val EOL = ByteBuffer.wrap("\r\n".getBytes(UTF8))
  val SimpleStringResponse = ByteBuffer.wrap("+".getBytes(UTF8))
  val ErrorResponse = ByteBuffer.wrap("-".getBytes(UTF8))
  val IntegerResponse = ByteBuffer.wrap(":".getBytes(UTF8))
  val BulkStringResponse = ByteBuffer.wrap("$".getBytes(UTF8))
  val ArrayResponse = ByteBuffer.wrap("*".getBytes(UTF8))
}

sealed trait RedisResponse { self: Product => }

sealed abstract class SingleLineResponse extends RedisResponse { self: Product => }
sealed abstract class MultiLineResponse extends RedisResponse { self: Product => }

case object NoResponse extends RedisResponse

case class StatusResponse(message: String) extends SingleLineResponse {
  override def toString: String = s"+$message\r\n"
}

case class ErrorResponse(message: String) extends SingleLineResponse {
  override def toString: String = s"-$message\r\n"
}

case class IntegerResponse(id: Long) extends SingleLineResponse {
  override def toString: String = s":$id\r\n"
}

case class BulkResponse(message: Array[Byte]) extends MultiLineResponse {
  override def toString: String = {
    s"$$${message.length}\r\n${new String(message, "UTF-8")}\r\n"
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case BulkResponse(other) =>
        other.length == message.length && {
          other.zip(message).forall { case (l, r) => l == r }
        }
      case _ => false
    }
  }
}

case object EmptyBulkResponse extends MultiLineResponse {
  override def toString: String = "$0\r\n\r\n"
}

case class MBulkResponse(messages: List[RedisResponse]) extends MultiLineResponse {
  override def toString: String = {
    s"*${messages.size}\r\n${messages.map(_.toString).mkString("")}"
  }
}

case object EmptyMBulkResponse extends MultiLineResponse {
  override def toString: String = "*0\r\n"
}
case object NilMBulkResponse extends MultiLineResponse {
  override def toString: String = s"*-1\r\n"
}
