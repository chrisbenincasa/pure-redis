package com.chrisbenincasa.redis.protocol

import cats.data.{State, StateT}
import com.chrisbenincasa.redis.protocol.ByteBufferImplicits._
import com.chrisbenincasa.redis.protocol.RedisDecoder.DecoderState
import com.chrisbenincasa.redis.protocol.RedisDecoderValue._
import java.nio.ByteBuffer

sealed trait RedisDecoderValue
object RedisDecoderValue {
  case class Error(err: Throwable) extends RedisDecoderValue
  case object Continue extends RedisDecoderValue
  case class Incomplete(state: RedisDecoderState, last: () => DecoderState) extends RedisDecoderValue
  case class ByteArrayValue(value: Array[Byte]) extends RedisDecoderValue
  case class StringValue(value: String) extends RedisDecoderValue
  case class RedisResponseValue(value: RedisResponse) extends RedisDecoderValue
}

case class RedisDecoderState(
  buffer: ByteBuffer,
  stack: List[Accumulation] = Nil
)

case class Accumulation(n: Int, r: List[RedisResponse], lastly: List[RedisResponse] => RedisResponse)

object RedisDecoder {
  type DecoderState = State[RedisDecoderState, RedisDecoderValue]

  private implicit def redisResponseAsValue(r: RedisResponse): RedisResponseValue = RedisResponseValue(r)

  private def state(f: RedisDecoderState => (RedisDecoderState, RedisDecoderValue)): DecoderState = State[RedisDecoderState, RedisDecoderValue](f)

  def readBytes(n: Int)(next: Array[Byte] => DecoderState): DecoderState = {
    RedisDecoder.state { case st @ RedisDecoderState(buf, _) =>
      if (buf.remaining() < n) {
        st -> Incomplete(st, () => readBytes(n)(next))
      } else {
        val arr = new Array[Byte](n)
        val ret = buf.slice().get(arr, 0, n)
        st.copy(buffer = ret) -> ByteArrayValue(arr)
      }
    }.flatMap {
      case ByteArrayValue(arr) => next(arr)
      case x => State.pure(x)
    }
  }

  def readLine(next: String => RedisDecoderValue): DecoderState =
    readLineS(x => State.pure(next(x)))

  def readLineS(next: String => DecoderState): DecoderState = {
    RedisDecoder.state { case st @ RedisDecoderState(buf, _) =>
      val bytesBeforeNewline = buf.findByteInBuf( '\n', buf.position(), buf.remaining())

      if (bytesBeforeNewline == -1) {
        st -> Incomplete(st, () => readLineS(next))
      } else {
        val arr = new Array[Byte](bytesBeforeNewline)
        buf.get(arr, 0, bytesBeforeNewline)
        val line = {
          if (arr.last == '\r') {
            new String(arr, 0, arr.length - 1, RedisResponse.UTF8)
          } else {
            new String(arr, RedisResponse.UTF8)
          }
        }

        buf.get()
        st -> StringValue(line)
      }
    }.flatMap {
      case StringValue(x) => next(x)
      case x => State.pure(x)
    }
  }

  private val decodeStatus: DecoderState =
    readLine(StatusResponse(_))

  private val decodeInteger: DecoderState =
    readLine(s => IntegerResponse(s.toLong))

  private val decodeError: DecoderState =
    readLine(line => ErrorResponse(line))

  private val decodeBulk: DecoderState = readLineS(line => {
    val numBytes = line.toInt

    if (numBytes < 0) {
      State.pure(EmptyBulkResponse)
    } else {
      readBytes(numBytes)(bytes => {
        readBytes(2) {
          case Array(x, y) if x == '\r'.toByte && y == '\n'.toByte =>
            State.pure(BulkResponse(bytes))

          case x =>
            State.pure(Error(new RuntimeException(s"Got unexpected byte array: ${new String(x)}")))
        }
      })
    }
  })

  private val decodeArrayStep: (RedisDecoderState, RedisDecoderValue) => DecoderState = {
    case (RedisDecoderState(_, acc :: tail), RedisResponseValue(v)) =>
      if (acc.n == 1) {
        State.modify[RedisDecoderState](_.copy(stack = tail)).
          map(_ => RedisResponseValue(acc.lastly(acc.r :+ v)))
      } else {
        State.modify[RedisDecoderState] { nextState =>
          val na = acc.copy(n = acc.n - 1, r = acc.r :+ v)
          nextState.copy(stack = na :: tail)
        }.map(_ => Continue)
      }

    case (_, Continue) =>
      State.pure(Continue)

    case (_, i @ Incomplete(_, last)) =>
      State.pure(i.copy(last = () => loop(last, unravelSingle)))

    case v =>
      State.pure(Error(new IllegalStateException(v.toString)))
  }

  private val unravelSingle: RedisDecoderValue => DecoderState = {
    incomingValue => {
      val handleIncoming = for {
        currState <- State.get[RedisDecoderState]
        newValue <- decodeArrayStep(currState, incomingValue)
      } yield newValue

      handleIncoming.flatMap {
        case RedisResponseValue(resp) => State.pure(resp)
        case _ =>
          for {
            decodedValue <- decode
            currState <- State.get[RedisDecoderState]
            result <- loop(() => decodeArrayStep(currState, decodedValue), unravelSingle)
          } yield result
      }
    }
  }

  private def loop(f: () => DecoderState, next: RedisDecoderValue => DecoderState): DecoderState =
    f().flatMap {
      case i @ Incomplete(_, redo) =>
        State.pure(i.copy(last = () => loop(redo, next))): DecoderState

      case RedisResponseValue(v @ (_: MBulkResponse | NilMBulkResponse | EmptyMBulkResponse)) =>
        State.pure(v)

      case v =>
        next(v)
    }

  private[this] val decodeArray: DecoderState = readLineS(line => {
    val numResps = line.toInt

    if (numResps < 0) {
      State.pure(NilMBulkResponse)
    } else if (numResps == 0) {
      State.pure(EmptyMBulkResponse)
    } else {
      RedisDecoder.state { st =>
        val newState = st.copy(stack = Accumulation(numResps, Nil, MBulkResponse.apply) +: st.stack)
        newState -> Continue
      }.flatMap(unravelSingle)
    }
  })

  private[this] val string: Byte = '+'
  private[this] val int: Byte = ':'
  private[this] val error: Byte = '-'
  private[this] val bulk: Byte = '$'
  private[this] val array: Byte = '*'

  val decodeFromMessageType: Array[Byte] => DecoderState = (b: Array[Byte]) => {
    if (b.isEmpty) {
      StateT.pure(EmptyMBulkResponse)
    } else {
      b.head match {
        case `string` => decodeStatus
        case `int` => decodeInteger
        case `error` => decodeError
        case `bulk` => decodeBulk
        case `array` => decodeArray
        case x => throw new IllegalStateException(s"Got byte = ${x.toChar}")
      }
    }
  }

  val decode: DecoderState = {
    readBytes(1)(decodeFromMessageType)
  }
}
