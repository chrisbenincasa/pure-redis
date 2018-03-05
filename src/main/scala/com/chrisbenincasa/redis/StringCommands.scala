package com.chrisbenincasa.redis

import cats.effect.IO
import com.chrisbenincasa.redis.protocol.{BulkResponse, EmptyBulkResponse, StatusResponse}
import com.chrisbenincasa.redis.protocol.commands.Get
import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}

private[redis] trait StringCommands { self: AbstractRedisClient =>
  def get(key: ByteBuffer)(implicit executionContext: ExecutionContext): IO[Option[ByteBuffer]] = {
    safeFork(Get(key)) {
      case BulkResponse(msg) => Future.successful(Some(ByteBuffer.wrap(msg)))
      case EmptyBulkResponse => Future.successful(None)
    }
  }

  def set(key: ByteBuffer, value: ByteBuffer)(implicit executionContext: ExecutionContext): IO[Unit] = {
    safeFork(com.chrisbenincasa.redis.protocol.commands.Set(key, value)) {
      case StatusResponse(_) => Future.unit
    }
  }
}
