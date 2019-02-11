package com.chrisbenincasa.redis

import cats.effect.Effect
import com.chrisbenincasa.redis.protocol._
import com.chrisbenincasa.redis.protocol.commands._
import java.nio.ByteBuffer

private[redis] trait StringCommands[F[_]] { self: AbstractRedisClient[F] =>
  def E: Effect[F]

  def get(key: ByteBuffer): F[Option[ByteBuffer]] =
    safeFork(Get(key)) {
      case BulkResponse(msg) => E.pure(Some(ByteBuffer.wrap(msg)))
      case EmptyBulkResponse => E.pure(None)
    }

  def set(key: ByteBuffer, value: ByteBuffer): F[Unit] =
    safeFork(Set(key, value)) {
      case StatusResponse(_) => E.unit
    }

  def incr(key: ByteBuffer): F[Long] =
    safeFork(Incr(key)) {
      case IntegerResponse(n) => E.pure(n)
    }

  def decr(key: ByteBuffer): F[Long] =
    safeFork(Decr(key)) {
      case IntegerResponse(n) => E.pure(n)
    }

  def mGet(keys: Seq[ByteBuffer]): F[Seq[Option[ByteBuffer]]] =
    safeFork(MGet(keys)) {
      case MBulkResponse(m) =>
        E.pure {
          m.map {
            case BulkResponse(s) => Some(ByteBuffer.wrap(s))
            case EmptyBulkResponse => None
            case _ => throw new IllegalStateException()
          }
        }
      case EmptyBulkResponse => E.pure(Seq.empty)
    }
}
