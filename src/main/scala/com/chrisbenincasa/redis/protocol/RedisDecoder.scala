package com.chrisbenincasa.redis.protocol

import cats.Eval
import cats.data.{State, StateT}
import java.nio.ByteBuffer

class RedisDecoder {
  val readBytes = (c: Int) => {
    State[ByteBuffer, Array[Byte]] { buf =>
      if (buf.remaining() < c) {
//        ???
        // TODO: FIX
        buf -> Array.emptyByteArray
      } else {
        val arr = new Array[Byte](c)
        val ret = buf.slice().get(arr, 0, c)
        ret -> arr
      }
    }
  }

  val readLine = {
    State[ByteBuffer, String] { buf =>
      val bytesBeforeNewline =
        findByteInBuf(buf, '\n', buf.position(), buf.remaining())
      if (bytesBeforeNewline == -1) {
        // TODO: fix
        ???
      } else {
        val arr = new Array[Byte](bytesBeforeNewline)
        buf.get(arr, 0, bytesBeforeNewline)
        val line = {
          val lastByte = arr(arr.length - 1)
          if (lastByte == '\r') {
            new String(arr, 0, arr.length - 1, RedisResponse.UTF8)
          } else {
            new String(arr, RedisResponse.UTF8)
          }
        }

        buf.get()
        buf -> line
      }
    }
  }

  private def findByteInBuf(buf: ByteBuffer,
                            search: Byte,
                            from: Int,
                            until: Int): Int = {
    require(from > 0 && until > 0)
    if (until <= from || from >= buf.remaining()) return -1
    val pos = buf.position()
    var i = from
    var continue = true
    val endAt = math.min(until, buf.remaining())
    while (continue && i < endAt) {
      val byte = buf.get(pos + i)
      if (byte != search) i += 1
      else continue = false
    }
    if (continue) -1
    else i
  }

  private[this] val decodeStatus: StateT[Eval, ByteBuffer, RedisResponse] = readLine.map(StatusResponse)

  private[this] val decodeInteger: StateT[Eval, ByteBuffer, RedisResponse] = readLine.map(s => IntegerResponse(s.toLong))

  private[this] val decodeError: StateT[Eval, ByteBuffer, RedisResponse] = readLine.map(ErrorResponse)

  private[this] val decodeBulk: StateT[Eval, ByteBuffer, RedisResponse] = readLine.flatMap(line => {
    val numBytes = line.toInt

    if (numBytes < 0) {
      StateT.pure(EmptyBulkResponse)
    } else {
      readBytes(numBytes).flatMap(bytes => {
        readBytes(2).map {
          case Array(x, y) if x == '\r'.toByte && y == '\n'.toByte => BulkResponse(bytes)
          case x => throw new RuntimeException(s"Got unexpected byte array: ${new String(x)}")
        }
      })
    }
  })

  private[this] val decodeArray: StateT[Eval, ByteBuffer, RedisResponse] = readLine.flatMap(line => {
    val numResps = line.toInt
    if (numResps < 0) StateT.pure(NilMBulkResponse)
    else if (numResps == 0) StateT.pure[Eval, ByteBuffer, RedisResponse](EmptyMBulkResponse)
    else {
      val empty = StateT.pure[Eval, ByteBuffer, List[RedisResponse]](Nil)
      (0 until numResps).foldLeft(empty) {
        case (state, _) => state.flatMap(resps => decode.map(resps :+ _))
      }.map(MBulkResponse)
    }
  })

  val string: Byte = '+'
  val int: Byte = ':'
  val error: Byte = '-'
  val bulk: Byte = '$'
  val array: Byte = '*'

  val decodeFromMessageType: Array[Byte] => StateT[Eval, ByteBuffer, RedisResponse] = (b: Array[Byte]) => {
    if (b.isEmpty) {
      StateT.pure(EmptyMBulkResponse)
    } else {
      b.head match {
        case `string` => decodeStatus
        case `int` => decodeInteger
        case `error` => decodeError
        case `bulk` => decodeBulk
        case `array` => decodeArray
        case _ => throw new IllegalStateException()
      }
    }

  }

  val decode: StateT[Eval, ByteBuffer, RedisResponse] = {
    readBytes(1).flatMap(decodeFromMessageType)
  }
}
