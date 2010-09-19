package voldemort.scalmert.client

import org.specs.Specification
import voldemort.serialization.StringSerializer
import scala.collection.mutable.Map

import voldemort.scalmert.Implicits._
import voldemort.scalmert.versioning._
import voldemort.client.MockStoreClientFactory

/**
 * @author alex
 */

class StoreClientSpec extends Specification {
  val serializer = new StringSerializer
  val factory = new MockStoreClientFactory(serializer, serializer)
  val client: StoreClient[String, String] = factory.getStoreClient[String, String]("test")

  "get" should {
    "return value put" in {
      client("hello") = "world"
      val actual = client.get("hello") getOrElse fail("get doesn't work after put")

      actual.getValue mustEqual "world"
    }
  }

  "getValue" should {
    "return the value put" in {
      client += ("hello", "world")
      val actual = client.getValue("hello") getOrElse fail("getValue doesn't work after put")

      actual mustEqual "world"
    }
  }

  "getAll" should {
    "return a map of keys to values" in {
      client("hello") = "world"
      client("foo") = "bar"
      val results = client.getAll(List("hello", "foo"))

      results("hello").getValue mustEqual "world"
      results("foo").getValue mustEqual "bar"
    }
  }

  "applyUpdate" should {
    "work with default maxTries" in {
      client.applyUpdate { c =>
        val v = c.get("moo") getOrElse Versioned("cow")
        c("moo") = v
      } mustBe true
      val v = client.getValue("moo") getOrElse fail("get doesn't work after applyUpdate")

      v mustEqual "cow"
    }

    "work with custom maxTries" in {
      client.applyUpdate(100) { c =>
        val v = c.get("meow") getOrElse Versioned("cat")
        c("meow") = v
      } mustBe true
      val v = client.getValue("meow") getOrElse fail("get doesn't work after applyUpdate")

      v mustEqual "cat"
    }
   }

  "asMap" should {
    "implement a mutable map" in {
      val map = client.asMap
      map.isInstanceOf[collection.mutable.Map[_, _]] must beTrue
      map += ("mutable" -> Versioned("map"))
      val v = map("mutable")
      v.getValue mustEqual "map"
      map -= "mutable"
      val exceptionCaught = try {
          client("mutable") mustBe None
          false
        } catch {
          case e => true
        }
      exceptionCaught mustBe true
    }
  }
}