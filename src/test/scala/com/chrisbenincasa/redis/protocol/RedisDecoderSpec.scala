package com.chrisbenincasa.redis.protocol

import cats.Eval
import com.chrisbenincasa.redis.protocol.RedisDecoderValue.RedisResponseValue
import java.nio.ByteBuffer
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Assertions, FunSuite}
import scala.util.Random

class RedisDecoderSpec extends FunSuite with GeneratorDrivenPropertyChecks with Assertions {
  val decoder = new RedisDecoder[Eval]
  val r = new Random()

  implicit val utf8String: Arbitrary[String] = {
    Arbitrary {
      Gen.choose(1, 512).map(size => r.alphanumeric.take(size).mkString(""))
    }
  }

  def genNonEmptyString: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
  def genStatusReply: Gen[StatusResponse] = genNonEmptyString.map(StatusResponse.apply)
  def genErrorReply: Gen[ErrorResponse] = genNonEmptyString.map(ErrorResponse.apply)
  def genIntegerReply: Gen[IntegerResponse] = Arbitrary.arbLong.arbitrary.map(IntegerResponse.apply)
  def genBulkReply: Gen[BulkResponse] = genNonEmptyString.map(_.getBytes("UTF-8")).map(BulkResponse.apply)

  def genFlatMBulkReply: Gen[MBulkResponse] =
    for {
      n <- Gen.choose(1, 10)
      line <- Gen.listOfN(
        n,
        Gen.oneOf(genStatusReply, genErrorReply, genIntegerReply, genBulkReply)
      )
    } yield MBulkResponse(line)

  def genMBulkReply(maxDeep: Int): Gen[MBulkResponse] =
    if (maxDeep == 0) genFlatMBulkReply
    else
      for {
        n <- Gen.choose(1, 10)
        line <- Gen.listOfN(
          n,
          Gen.oneOf(
            genStatusReply,
            genErrorReply,
            genIntegerReply,
            genBulkReply,
            genMBulkReply(maxDeep - 1)
          )
        )
      } yield MBulkResponse(line)

  test("Simple strings") {
    forAll(genStatusReply) { r: StatusResponse =>
      val encoded = r.toString.getBytes()
      decoder.decode.runA(RedisDecoderState(ByteBuffer.wrap(encoded))).value match {
        case RedisResponseValue(StatusResponse(msg)) => assert(msg === r.message)
        case x => fail(s"Expected StatusReply but got $x")
      }
    }
  }

  test("Errors") {
    forAll(genErrorReply) { r: ErrorResponse =>
      val encoded = r.toString.getBytes()
      decoder.decode.runA(RedisDecoderState(ByteBuffer.wrap(encoded))).value match {
        case RedisResponseValue(ErrorResponse(msg)) => assert(msg === r.message)
        case x => fail(s"Expected ax ErrorReply but got $x")
      }
    }
  }

  test("Ints") {
    forAll(genIntegerReply) { l: IntegerResponse => {
      decoder.decode.runA(RedisDecoderState(ByteBuffer.wrap(l.toString.getBytes()))).value match {
        case RedisResponseValue(IntegerResponse(msg)) => assert(msg === l.id)
        case x => fail(s"Expected a IntegerReply but got $x")
      }
    }}
  }

  test("Bulk string") {
    forAll(genBulkReply) { s: BulkResponse =>
      decoder.decode.runA(RedisDecoderState(ByteBuffer.wrap(s.toString.getBytes))).value match {
        case RedisResponseValue(BulkResponse(bytes)) => assert(new String(bytes) === new String(s.message))
        case x => fail(s"Expected a BulkReply but got $x")
      }
    }
  }

  test("Arrays") {
    forAll(genMBulkReply(10)) { msg: MBulkResponse =>
      val res = decoder.decode.runA(RedisDecoderState(ByteBuffer.wrap(msg.toString.getBytes()))).value
      res match {
        case RedisResponseValue(mb @ MBulkResponse(_)) => assert(mb === msg)
        case x => fail(s"Expected a MBulkReply but got $x")
      }
    }
  }
}
