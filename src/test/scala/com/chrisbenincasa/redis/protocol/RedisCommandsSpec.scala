package com.chrisbenincasa.redis.protocol

import cats.effect.IO
import com.chrisbenincasa.redis.BaseRedisClient
import com.chrisbenincasa.redis.protocol.ByteBufferImplicits._
import com.chrisbenincasa.redis.protocol.commands.{Command, EncodedCommandString, Get}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.concurrent.{ExecutorService, Executors}
import org.scalatest.FunSuite
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RedisCommandsSpec extends FunSuite {
  private implicit val executor = Executors.newSingleThreadExecutor()

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

    val (k, k2) = Await.result(io.unsafeToFuture(), 5 seconds)

    assert(new String(k.get.array()) == "1")
    assert(k2 == 2)
  }
}

class SocketRedisClient(addr: InetSocketAddress = new InetSocketAddress("localhost", 6379))(implicit E: ExecutorService)
  extends BaseRedisClient {

  private val channel = SocketChannel.open(addr)
  private val decoder = new RedisDecoder

  override protected def makeRequest(command: Command): IO[RedisResponse] = {
    val encoded = Command.encode(command)
    val wholeBuf = ByteBuffer.allocate(encoded.map(_.length).sum)
    wholeBuf.clear()
    encoded.foreach(wholeBuf.put)
    wholeBuf.flip()

    IO {
      while (wholeBuf.hasRemaining) {
        channel.write(wholeBuf)
      }
    }.flatMap(_ => {
      val readBuffer = ByteBuffer.allocate(512)
      readBuffer.clear()
      channel.read(readBuffer)
      readBuffer.flip()

      IO.eval(decoder.decode.runA(readBuffer))
    })
  }
}
