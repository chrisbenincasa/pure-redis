package com.chrisbenincasa.redis

import cats.effect.IO
import com.chrisbenincasa.redis.protocol._
import com.chrisbenincasa.redis.protocol.commands.Command
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

abstract class BaseRedisClient
  extends AbstractRedisClient
    with StringCommands {}

abstract class AbstractRedisClient {
  protected def makeRequest(cmd: Command): IO[RedisResponse]

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
