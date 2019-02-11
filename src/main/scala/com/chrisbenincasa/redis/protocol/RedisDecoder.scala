package com.chrisbenincasa.redis.protocol

import cats.data.StateT
import cats.{Applicative, FlatMap, Functor, Monad}
import com.chrisbenincasa.redis.protocol.ByteBufferImplicits._
import com.chrisbenincasa.redis.protocol.RedisDecoderValue._
import java.nio.ByteBuffer

sealed trait RedisDecoderValue
object RedisDecoderValue {
  case class Error(err: Throwable) extends RedisDecoderValue
  case object Continue extends RedisDecoderValue
  case class Incomplete[F[_]: Monad : FlatMap](state: RedisDecoderState, last: () => RedisDecoder.DecoderState[F]) extends RedisDecoderValue
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
  type DecoderState[F[_]] = StateT[F, RedisDecoderState, RedisDecoderValue]
}

class RedisDecoder[F[_] : Monad : FlatMap] {
  import RedisDecoder.DecoderState

  private val AP = Applicative[F]
  private val F = Functor[F]

  private implicit def redisResponseAsValue(r: RedisResponse): RedisResponseValue = RedisResponseValue(r)

  private def state(f: RedisDecoderState => F[(RedisDecoderState, RedisDecoderValue)]): DecoderState[F] =
    StateT[F, RedisDecoderState, RedisDecoderValue](f)

  private def pureF[X](x: X): F[X] =
    F.widen(AP.pure(x))

  private def pureState(x: RedisDecoderValue): DecoderState[F] =
    StateT.pure(x)

  def readBytes(n: Int)(next: Array[Byte] => DecoderState[F]): DecoderState[F] = {
    state { case st @ RedisDecoderState(buf, _) =>
      if (buf.remaining() < n) {
        pureF(st -> Incomplete(st, () => readBytes(n)(next)))
      } else {
        val arr = new Array[Byte](n)
        val ret = buf.slice().get(arr, 0, n)
        pureF(st.copy(buffer = ret) -> ByteArrayValue(arr))
      }
    }.flatMap {
      case ByteArrayValue(arr) => next(arr)
      case x => StateT.pure(x)
    }
  }

  def readLine(next: String => RedisDecoderValue): DecoderState[F] =
    readLineS(x => StateT.pure(next(x)))

  def readLineS(next: String => DecoderState[F]): DecoderState[F] = {
    state { case st @ RedisDecoderState(buf, _) =>
      val bytesBeforeNewline = buf.findByteInBuf( '\n', buf.position(), buf.remaining())

      if (bytesBeforeNewline == -1) {
        pureF(st -> Incomplete(st, () => readLineS(next)))
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
        pureF(st -> StringValue(line))
      }
    }.flatMap {
      case StringValue(x) => next(x)
      case x => StateT.pure(x)
    }
  }

  private val decodeStatus: DecoderState[F] =
    readLine(StatusResponse.apply)

  private val decodeInteger: DecoderState[F] =
    readLine(s => IntegerResponse(s.toLong))

  private val decodeError: DecoderState[F] =
    readLine(line => ErrorResponse(line))

  private val decodeBulk: DecoderState[F] = readLineS(line => {
    val numBytes = line.toInt

    if (numBytes < 0) {
      pureState(EmptyBulkResponse)
    } else {
      readBytes(numBytes)(bytes => {
        readBytes(2) {
          case Array(x, y) if x == '\r'.toByte && y == '\n'.toByte =>
            pureState(BulkResponse(bytes))

          case x =>
            pureState(Error(new RuntimeException(s"Got unexpected byte array: ${new String(x)}")))
        }
      })
    }
  })

  private val decodeArrayStep: (RedisDecoderState, RedisDecoderValue) => DecoderState[F] = {
    case (RedisDecoderState(_, acc :: tail), RedisResponseValue(v)) =>
      if (acc.n == 1) {
        StateT.modify[F, RedisDecoderState](_.copy(stack = tail)).
          map(_ => RedisResponseValue(acc.lastly(acc.r :+ v)))
      } else {
        StateT.modify[F, RedisDecoderState] { nextState =>
          val na = acc.copy(n = acc.n - 1, r = acc.r :+ v)
          nextState.copy(stack = na :: tail)
        }.map(_ => Continue)
      }

    case (_, Continue) =>
      StateT.pure(Continue)

    case (_, i: Incomplete[F]) =>
      StateT.pure(i.copy(last = () => loop(i.last, unravelSingle)))

    case v =>
      StateT.pure(Error(new IllegalStateException(v.toString)))
  }

  private lazy val unravelSingle: RedisDecoderValue => DecoderState[F] = {
    incomingValue => {
      val handleIncoming = for {
        currState <- StateT.get[F, RedisDecoderState]
        newValue <- decodeArrayStep(currState, incomingValue)
      } yield newValue

      handleIncoming.flatMap {
        case RedisResponseValue(resp) => pureState(resp)
        case _ =>
          for {
            decodedValue <- decode
            currState <- StateT.get[F, RedisDecoderState]
            result <- loop(() => decodeArrayStep(currState, decodedValue), unravelSingle)
          } yield result
      }
    }
  }

  private def loop(f: () => DecoderState[F], next: RedisDecoderValue => DecoderState[F]): DecoderState[F] =
    f().flatMap {
      case i: Incomplete[F] =>
        pureState(i.copy(last = () => loop(i.last, next)))

      case RedisResponseValue(v @ (_: MBulkResponse | NilMBulkResponse | EmptyMBulkResponse)) =>
        pureState(v)

      case v =>
        next(v)
    }

  private[this] val decodeArray: DecoderState[F] = readLineS(line => {
    val numResps = line.toInt

    if (numResps < 0) {
      pureState(NilMBulkResponse)
    } else if (numResps == 0) {
      pureState(EmptyMBulkResponse)
    } else {
      state { st =>
        val newState = st.copy(stack = Accumulation(numResps, Nil, MBulkResponse.apply) +: st.stack)
        pureF(newState -> Continue)
      }.flatMap(unravelSingle)
    }
  })

  private[this] val string: Byte = '+'
  private[this] val int: Byte = ':'
  private[this] val error: Byte = '-'
  private[this] val bulk: Byte = '$'
  private[this] val array: Byte = '*'

  val decodeFromMessageType: Array[Byte] => DecoderState[F] = (b: Array[Byte]) => {
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

  lazy val decode: DecoderState[F] = {
    readBytes(1)(decodeFromMessageType)
  }
}
