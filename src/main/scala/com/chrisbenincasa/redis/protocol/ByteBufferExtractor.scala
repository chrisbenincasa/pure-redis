package com.chrisbenincasa.redis.protocol

import java.nio.ByteBuffer

object ByteBufferExtractor {
  def unapply(buf: ByteBuffer): Option[java.nio.ByteBuffer] =
    Some(buf.asReadOnlyBuffer)
}

object ByteArrayExtractor {
  def unapply(byteArray: Array[Byte]): Option[Array[Byte]] = {
    Some(byteArray)
  }
}
