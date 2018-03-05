package com.chrisbenincasa.redis.protocol.commands

import java.nio.ByteBuffer
import com.chrisbenincasa.redis.protocol.ByteBufferImplicits._

abstract class Command {
  def name: ByteBuffer
  def body: Seq[ByteBuffer] = Seq.empty
}

object Command {
  val APPEND: ByteBuffer = "APPEND"
  val BITCOUNT: ByteBuffer = "BITCOUNT"
  val DECR: ByteBuffer = "DECR"
  val DECRBY: ByteBuffer = "DECRBY"
  val GET: ByteBuffer = "GET"
  val GETBIT: ByteBuffer = "GETBIT"
  val GETRANGE: ByteBuffer = "GETRANGE"
  val GETSET: ByteBuffer = "GETSET"
  val INCR: ByteBuffer = "INCR"
  val INCRBY: ByteBuffer = "INCRBY"
  val INCRBYFLOAT: ByteBuffer = "INCRBYFLOAT"
  val MSET: ByteBuffer = "MSET"
  val MSETNX: ByteBuffer = "MSETNX"
  val PSETEX: ByteBuffer = "PSETEX"
  val SET: ByteBuffer = "SET"
  val SETBIT: ByteBuffer = "SETBIT"
  val SETEX: ByteBuffer = "SETEX"
  val SETNX: ByteBuffer = "SETNX"
  val SETRANGE: ByteBuffer = "SETRANGE"
  val STRLEN: ByteBuffer = "STRLEN"

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


