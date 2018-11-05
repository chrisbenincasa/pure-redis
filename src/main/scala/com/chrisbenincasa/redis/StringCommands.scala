package com.chrisbenincasa.redis

import cats.effect.IO
import com.chrisbenincasa.redis.protocol.commands._
import com.chrisbenincasa.redis.protocol.{BulkResponse, EmptyBulkResponse, IntegerResponse, StatusResponse}
import java.nio.ByteBuffer
import scala.concurrent.ExecutionContext

private[redis] trait StringCommands { self: AbstractRedisClient =>
  def get(key: ByteBuffer)(implicit E: ExecutionContext): IO[Option[ByteBuffer]] =
    safeFork(Get(key)) {
      case BulkResponse(msg) => IO.pure(Some(ByteBuffer.wrap(msg)))
      case EmptyBulkResponse => IO.pure(None)
    }

  def set(key: ByteBuffer, value: ByteBuffer)(implicit E: ExecutionContext): IO[Unit] =
    safeFork(Set(key, value)) {
      case StatusResponse(_) => IO.unit
    }

  def incr(key: ByteBuffer)(implicit E: ExecutionContext): IO[Long] =
    safeFork(Incr(key)) {
      case IntegerResponse(n) => IO.pure(n)
    }

  def decr(key: ByteBuffer)(implicit E: ExecutionContext): IO[Long] =
    safeFork(Decr(key)) {
      case IntegerResponse(n) => IO.pure(n)
    }
}
