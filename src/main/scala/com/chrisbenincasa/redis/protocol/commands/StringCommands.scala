package com.chrisbenincasa.redis.protocol.commands

import java.nio.ByteBuffer
import com.chrisbenincasa.redis.protocol.ByteBufferImplicits._
import scala.concurrent.duration.FiniteDuration

case class Append(key: ByteBuffer, value: ByteBuffer) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.APPEND
  override def body: Seq[ByteBuffer] = Seq(key, value)
}

case class BitCount(key: ByteBuffer, start: Option[Int] = None, end: Option[Int] = None) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.BITCOUNT
  override def body: Seq[ByteBuffer] =
    Seq(key) ++
      start.map(_.toString).map(stringToByteBuffer).toSeq ++
      end.map(_.toString).map(stringToByteBuffer).toSeq
}

case class BitOp()

case class Decr(key: ByteBuffer) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.DECR
  override def body: Seq[ByteBuffer] = Seq(key)
}

case class DecrBy(key: ByteBuffer, amount: Long) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.DECRBY
  override def body: Seq[ByteBuffer] = Seq(key, amount.toString)
}

case class Get(key: ByteBuffer) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.GET
}

case class GetBit(key: ByteBuffer, offset: Int) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.GETBIT
  override def body: Seq[ByteBuffer] = Seq(key, offset.toString)
}

case class GetRange(key: ByteBuffer, start: Int, end: Int) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.GETRANGE
  override def body: Seq[ByteBuffer] = Seq(key, start.toString, end.toString)
}

case class GetSet(key: ByteBuffer, value: Int) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.GETSET
  override def body: Seq[ByteBuffer] = Seq(key, value.toString)
}

case class Incr(key: ByteBuffer) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.INCR
}

case class IncrBy(key: ByteBuffer, by: Int) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.INCRBY
  override def body: Seq[ByteBuffer] = Seq(key, by.toString)
}

case class IncrByFloat(key: ByteBuffer, by: Float) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.INCRBYFLOAT
  override def body: Seq[ByteBuffer] = Seq(key, by.toString)
}

case class MGet(keys: Seq[ByteBuffer]) extends MultiKeyBasedCommand {
  override def name: ByteBuffer = Command.INCRBYFLOAT
  override def body: Seq[ByteBuffer] = keys
}

trait MultiSetCommand extends MultiKeyBasedCommand {
  def kv: Map[ByteBuffer, ByteBuffer]
  def keys: Seq[ByteBuffer] = kv.keys.toSeq
  override def body: Seq[ByteBuffer] = kv.foldLeft(Seq.empty[ByteBuffer]) { case (ac, (b1, b2)) => ac ++ Seq(b1, b2) }
}

case class MSet(kv: Map[ByteBuffer, ByteBuffer]) extends MultiSetCommand {
  override def name: ByteBuffer = Command.MSET
}

case class MSetNx(kv: Map[ByteBuffer, ByteBuffer]) extends MultiSetCommand {
  override def name: ByteBuffer = Command.MSETNX
}

case class PSetEx(key: ByteBuffer, millis: Long, value: ByteBuffer) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.PSETEX
  override def body: Seq[ByteBuffer] = Seq(key, millis.toString, value)
}

case class Set(
  key: ByteBuffer,
  value: ByteBuffer,
  ttl: Option[FiniteDuration] = None,
  nx: Boolean = false,
  xx: Boolean = false
) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.SET
  override def body: Seq[ByteBuffer] = {
    val kv = Seq(key, value)
    val ttlPart: Option[Seq[ByteBuffer]] = ttl.map(_.toMillis).map(ms => Seq(Set.PX, ms.toString))
    val nxxx = (if (nx) Seq(Set.NX) else Seq()) ++ (if (xx) Seq(Set.XX) else Seq())

    kv ++ ttlPart.getOrElse(Seq()) ++ nxxx
  }
}

object Set {
  private val EX: ByteBuffer = "EX"
  private val PX: ByteBuffer = "PX"
  private val XX: ByteBuffer = "XX"
  private val NX: ByteBuffer = "NX"
}

case class SetBit(key: ByteBuffer, offset: Int, value: Int) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.SETBIT
  override def body: Seq[ByteBuffer] = Seq(key, offset.toString, value.toString)
}

case class SetEx(key: ByteBuffer, seconds: Long, value: ByteBuffer) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.SETEX
  override def body: Seq[ByteBuffer] = Seq(key, seconds.toString, value)
}

case class SetNx(key: ByteBuffer, value: ByteBuffer) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.SETNX
  override def body: Seq[ByteBuffer] = Seq(key, value)
}

case class SetRange(key: ByteBuffer, offset: Int, value: ByteBuffer) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.SETRANGE
  override def body: Seq[ByteBuffer] = Seq(key, offset.toString, value)
}

case class Strlen(key: ByteBuffer) extends KeyBasedCommand {
  override def name: ByteBuffer = Command.STRLEN
}
