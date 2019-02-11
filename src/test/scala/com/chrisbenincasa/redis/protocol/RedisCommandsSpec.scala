package com.chrisbenincasa.redis.protocol

import cats.effect.{ContextShift, Effect, IO}
import com.chrisbenincasa.redis.BaseRedisClient
import com.chrisbenincasa.redis.protocol.ByteBufferImplicits._
import com.chrisbenincasa.redis.protocol.commands.{Command, EncodedCommandString, Get}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.concurrent.Executors
import org.scalatest.FunSuite
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class RedisCommandsSpec extends FunSuite {
  private implicit val executor = Executors.newSingleThreadExecutor()
  private implicit val ctx = IO.contextShift(ExecutionContext.fromExecutor(executor))

  test("GET") {
    Command.encode(Get("key")) match {
      case EncodedCommandString(x) =>
        println(x)
    }
  }

  test("Client") {
    val client = new SocketRedisClient

    val io = for {
      _ <- client.set("key", "1")
      k <- client.get("key")
      k2 <- client.incr("key")
    } yield k -> k2

    val (k, k2) = Await.result(io.unsafeToFuture(), Duration.Inf)

    assert(new String(k.get.array()) == "1")
    assert(k2 == 2)
  }

  test("Reentrant array") {
    val client = new SocketRedisClient(readBufferSize = 2)

    val io = for {
      _ <- client.set("key", "1")
      _ <- client.set("key2", "2")
      _ <- client.set("key3", "3")
      _ <- client.set("key4", "4")
      all <- client.mGet(Seq("key", "key2", "key3", "key4", "not"))
    } yield all

    val all = Await.result(io.unsafeToFuture(), Duration.Inf)

    println(all.map(_.map(byteBufferToString)))
  }
}

class SocketRedisClient(
  addr: InetSocketAddress = new InetSocketAddress("localhost", 6379),
  readBufferSize: Int = 512
)(implicit ctx: ContextShift[IO], eff: Effect[IO])
  extends BaseRedisClient[IO] {

  private val channel = SocketChannel.open(addr)

  override protected def send(command: Command): IO[Unit] = {
    val encoded = Command.encode(command)
    val wholeBuf = ByteBuffer.allocate(encoded.map(_.length).sum)
    wholeBuf.clear()
    encoded.foreach(wholeBuf.put)
    wholeBuf.flip()

    IO {
      while (wholeBuf.hasRemaining) {
        channel.write(wholeBuf)
      }
    }
  }

  override protected def read(): IO[ByteBuffer] = {
    val readBuffer = ByteBuffer.allocate(readBufferSize)
    for {
      _ <- IO.pure(readBuffer.clear())
      _ <- IO(channel.read(readBuffer))
      _ <- IO.pure(readBuffer.flip())
    } yield readBuffer
  }
}
