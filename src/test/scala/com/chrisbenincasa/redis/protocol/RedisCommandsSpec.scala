package com.chrisbenincasa.redis.protocol

import com.chrisbenincasa.redis.BaseRedisClient
import com.chrisbenincasa.redis.protocol.ByteBufferImplicits._
import com.chrisbenincasa.redis.protocol.commands.{Command, EncodedCommandString, Get}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.concurrent.{ExecutorService, Executors}
import org.scalatest.FunSuite
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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

    val io = client.set("key", "1").flatMap(_ => client.get("key"))

    val p = Await.result(io.unsafeToFuture(), Duration.Inf)

    println(new String(p.get.array()))
  }
}

class SocketRedisClient(addr: InetSocketAddress = new InetSocketAddress("localhost", 6379))(implicit E: ExecutorService)
  extends BaseRedisClient {

  private val channel = SocketChannel.open(addr)
  private val decoder = new RedisDecoder

  override protected def makeRequest(command: Command): Future[RedisResponse] = {
    val encoded = Command.encode(command)
    val wholeBuf = ByteBuffer.allocate(encoded.map(_.length).sum)
    wholeBuf.clear()
    encoded.foreach(wholeBuf.put)
    wholeBuf.flip()

    while (wholeBuf.hasRemaining) {
      channel.write(wholeBuf)
    }

    val readBuffer = ByteBuffer.allocate(512)
    readBuffer.clear()
    channel.read(readBuffer)
    readBuffer.flip()

    val result = decoder.decode.runA(readBuffer).value
    Future.successful(result)
  }
}
