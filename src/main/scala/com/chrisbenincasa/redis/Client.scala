package com.chrisbenincasa.redis

import cats.effect.{ConcurrentEffect, ContextShift, Effect}
import cats.syntax.all._
import com.chrisbenincasa.redis.protocol.ByteBufferImplicits._
import com.chrisbenincasa.redis.protocol.RedisDecoderValue.{Error, Incomplete, RedisResponseValue}
import com.chrisbenincasa.redis.protocol._
import com.chrisbenincasa.redis.protocol.commands.Command
import java.nio.ByteBuffer

abstract class BaseRedisClient[F[_]: ConcurrentEffect](implicit ctx: ContextShift[F], EF: Effect[F])
  extends AbstractRedisClient[F]
    with StringCommands[F] {
    val E: Effect[F] = EF
  }

abstract class AbstractRedisClient[F[_]: ConcurrentEffect](implicit CS: ContextShift[F], E: Effect[F]) {

  protected val decoder = new RedisDecoder[F]

  protected def makeRequest(command: Command): F[RedisResponse] = {
    for {
      _ <- send(command)
      response <- readAndParse()
    } yield response
  }

  protected def send(command: Command): F[Unit]

  protected def read(): F[ByteBuffer]

  private def readAndParse(prev: Option[Incomplete[F]] = None): F[RedisResponse] = {
    for {
      readBuf <- read()
      concatedBuf <- E.delay(prev.map(_.state).map(_.buffer.concat(readBuf)).getOrElse(readBuf))
      decodingResult <- {
        val state = prev.map(_.last()).getOrElse(decoder.decode)
        val initial = prev.map(_.state.copy(buffer = concatedBuf)).getOrElse(RedisDecoderState(concatedBuf))
        state.run(initial).map(_._2)
      }
      redisResponse <- decodingResult match {
        case i: Incomplete[F] => E.suspend(readAndParse(Some(i)))
        case RedisResponseValue(rr) => E.pure(rr)
        case Error(err) => E.raiseError(err)
        case x => E.raiseError(new IllegalStateException(s"Illegal state reached: $x"))
      }
    } yield redisResponse
  }

  protected def safeFork[T](command: Command)(handle: PartialFunction[RedisResponse, F[T]]): F[T] = {
    CS.shift *> request(command)(handle)
  }

  private[redis] def request[T](
    command: Command
  )(handleResponse: PartialFunction[RedisResponse, F[T]]): F[T] = {
    makeRequest(command).flatMap(handleResponse orElse {
      case ErrorResponse(message) => E.raiseError(new RuntimeException(message))
      case r => E.raiseError(new IllegalStateException(r.toString))
    })
  }
}
