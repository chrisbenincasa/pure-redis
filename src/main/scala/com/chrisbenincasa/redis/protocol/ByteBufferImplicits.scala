package com.chrisbenincasa.redis.protocol

import java.nio.ByteBuffer

object ByteBufferImplicits {
  implicit def arrayToByteBuf(arr: Array[Byte]): ByteBuffer = ByteBuffer.wrap(arr)
  implicit def stringToByteBuffer(s: String): ByteBuffer = ByteBuffer.wrap(s.getBytes)
  implicit def byteBufferToString(bb: ByteBuffer): String = new String(bb.array(), "UTF-8")

  implicit class RichByteBuffer(buf: ByteBuffer) {
    def findByteInBuf(search: Byte, from: Int, until: Int): Int = {
      if ((from <= 0 && until <= 0) || until <= from || from >= buf.remaining()) {
        -1
      } else {
        buf.synchronized {
          buf.mark()

          val pos = buf.position()
          var i = from
          var continue = true
          val endAt = math.min(until, buf.remaining())
          while (continue && i < endAt) {
            val byte = buf.get(pos + i)
            if (byte != search) {
              i += 1
            } else {
              continue = false
            }
          }

          buf.reset()

          if (continue) -1 else i
        }
      }
    }

    def concat(other: ByteBuffer): ByteBuffer = {
      val b = ByteBuffer.allocate(buf.remaining() + other.remaining()).put(buf).put(other)
      b.flip()
      b
    }
  }
}
