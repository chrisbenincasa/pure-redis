package com.chrisbenincasa.redis.protocol.commands

import java.nio.ByteBuffer
import com.chrisbenincasa.redis.protocol.ByteBufferImplicits._

abstract class Command {
  def name: ByteBuffer
  def body: Seq[ByteBuffer] = Seq.empty
}

object Command {
  val APPEND: Array[Byte] = "APPEND".getBytes()
  val BITCOUNT: Array[Byte] = "BITCOUNT".getBytes()
  val DECR: Array[Byte] = "DECR".getBytes()
  val DECRBY: Array[Byte] = "DECRBY".getBytes()
  val GET: Array[Byte] = "GET".getBytes()
  val GETBIT: Array[Byte] = "GETBIT".getBytes()
  val GETRANGE: Array[Byte] = "GETRANGE".getBytes()
  val GETSET: Array[Byte] = "GETSET".getBytes()
  val INCR: Array[Byte] = "INCR".getBytes()
  val INCRBY: Array[Byte] = "INCRBY".getBytes()
  val INCRBYFLOAT: Array[Byte] = "INCRBYFLOAT".getBytes()
  val MSET: Array[Byte] = "MSET".getBytes()
  val MGET: Array[Byte] = "MGET".getBytes()
  val MSETNX: Array[Byte] = "MSETNX".getBytes()
  val PSETEX: Array[Byte] = "PSETEX".getBytes()
  val SET: Array[Byte] = "SET".getBytes()
  val SETBIT: Array[Byte] = "SETBIT".getBytes()
  val SETEX: Array[Byte] = "SETEX".getBytes()
  val SETNX: Array[Byte] = "SETNX".getBytes()
  val SETRANGE: Array[Byte] = "SETRANGE".getBytes()
  val STRLEN: Array[Byte] = "STRLEN".getBytes()

  def encode(command: Command): Seq[ByteBuffer] = {
    val commandArgs = command.name +: command.body
    val preamble: Seq[ByteBuffer] = Seq("*", commandArgs.size.toString, "\r\n")
    val bufs = commandArgs.flatMap(arg => Seq[ByteBuffer]("$", arg.length.toString, "\r\n", arg, "\r\n"))

    preamble ++ bufs
  }
}

object EncodedCommandString {
  def unapply(encodedCommands: Seq[ByteBuffer]): Option[String] = {
    Some(encodedCommands.map(byteBufferToString).reduceLeftOption(_ + _).getOrElse(""))
  }
}

trait KeyBasedCommand extends Command {
  def key: ByteBuffer
  override def body: Seq[ByteBuffer] = Seq(key)
}

trait MultiKeyBasedCommand extends Command {
  def keys: Seq[ByteBuffer]
}


