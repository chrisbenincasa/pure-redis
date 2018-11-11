package com.chrisbenincasa.redis

import cats.effect.IO
import com.chrisbenincasa.redis.protocol.ByteBufferImplicits._
import com.chrisbenincasa.redis.protocol.RedisDecoderValue.{Incomplete, RedisResponseValue}
import com.chrisbenincasa.redis.protocol._
import com.chrisbenincasa.redis.protocol.commands.Command
import java.nio.ByteBuffer
import scala.concurrent.ExecutionContext

abstract class BaseRedisClient
  extends AbstractRedisClient
    with StringCommands {}

abstract class AbstractRedisClient {
  protected def makeRequest(command: Command): IO[RedisResponse] = {
    for {
      _ <- send(command)
      res <- readAndParse()
    } yield res
  }

  protected def send(command: Command): IO[Unit]

  protected def read(): IO[ByteBuffer]

  private def readAndParse(prev: Option[Incomplete] = None): IO[RedisResponse] = {
    for {
      buf <- read()
      concat <- IO(prev.map(_.state).map(_.buffer.concat(buf)).getOrElse(buf))
      _ <- IO(println("buf " + new String(concat.duplicate().array()).replaceAll("\n", "\\\\n").replaceAll("\r", "\\\\r")))
      res <- IO.eval {
        val eval = prev.map(_.last()).getOrElse(RedisDecoder.decode)
        val state = prev.map(_.state.copy(buffer = concat)).getOrElse(RedisDecoderState(concat))
        eval.runA(state)
      }
      q <- res match {
        case i: Incomplete => IO.suspend(readAndParse(Some(i)))
        case RedisResponseValue(rr) => IO.pure(rr)
        case x => IO.raiseError(new IllegalStateException(x.toString))
      }
    } yield q
  }

  protected def safeFork[T](command: Command)(handle: PartialFunction[RedisResponse, IO[T]])(implicit executionContext: ExecutionContext): IO[T] = {
    IO.shift(executionContext).flatMap(_ => request(command)(handle))
  }

  private[redis] def request[T](
    command: Command
  )(handleResponse: PartialFunction[RedisResponse, IO[T]])(implicit executionContext: ExecutionContext): IO[T] = {
    makeRequest(command).flatMap(handleResponse orElse {
      case ErrorResponse(message) => IO.raiseError(new RuntimeException(message))
      case r => IO.raiseError(new IllegalStateException(r.toString))
    })
  }
}
