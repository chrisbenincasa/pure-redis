package com.chrisbenincasa.redis

import cats.effect.IO
import com.chrisbenincasa.redis.protocol._
import com.chrisbenincasa.redis.protocol.commands.Command
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

abstract class BaseRedisClient
  extends AbstractRedisClient
    with StringCommands {}

abstract class AbstractRedisClient {
  protected def makeRequest(cmd: Command): Future[RedisResponse]

  protected def safeFork[T](command: Command)(handle: PartialFunction[RedisResponse, Future[T]])(implicit executionContext: ExecutionContext): IO[T] = {
    IO.async[T] { cb =>
      try {
        request(command)(handle).
          map(r => cb(Right(r))).recover {
          case NonFatal(ex) => cb(Left(ex))
        }
      } catch {
        case NonFatal(e) => cb(Left(e))
      }
    }
  }

  private[redis] def request[T](
    command: Command
  )(handleResponse: PartialFunction[RedisResponse, Future[T]])(implicit executionContext: ExecutionContext): Future[T] = {
    makeRequest(command).flatMap(handleResponse orElse {
      case ErrorResponse(message) => Future.failed(new RuntimeException(message))
      case _ => Future.failed(new IllegalStateException)
    })
  }
}
