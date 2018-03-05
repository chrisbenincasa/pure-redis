package com.chrisbenincasa.redis.protocol

import java.nio.ByteBuffer

object ByteBufferImplicits {
  implicit def stringToByteBuffer(s: String): ByteBuffer = ByteBuffer.wrap(s.getBytes)
  implicit def byteBufferToString(bb: ByteBuffer): String = new String(bb.array(), "UTF-8")
}
