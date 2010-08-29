package voldemort.scalmert.client

import org.specs.Specification
import voldemort.serialization.StringSerializer
import scala.collection.mutable.Map

import voldemort.scalmert.Implicits._
import voldemort.client.MockStoreClientFactory
import voldemort.versioning.Versioned

/**
 * @author alex
 */

class StoreClientSpec extends Specification {
  val serializer = new StringSerializer
  val factory = new MockStoreClientFactory(serializer, serializer)
  val client: StoreClient[String, String] = factory.getStoreClient[String, String]("test")

  "get" should {
    "return value put" in {
      client.put("hello", "world")
      val actual = client.get("hello") getOrElse fail("get doesn't work after put")

      actual.getValue mustEqual "world"
    }
  }

  "getValue" should {
    "return the value put" in {
      client.put("hello", "world")
      val actual = client.getValue("hello") getOrElse fail("getValue doesn't work after put")

      actual mustEqual "world"
    }
  }

  "getAll" should {
    "return a map of keys to values" in {
      client.put("hello", "world")
      client.put("foo", "bar")
      val results = client.getAll(List("hello", "foo"))

      results("hello").getValue mustEqual "world"
      results("foo").getValue mustEqual "bar"
    }
  }

  "applyUpdate" should {
    "work with default maxTries" in {
      client.applyUpdate { c =>
        val v = c.get("moo") getOrElse new Versioned[String]("cow")
        c.put("moo", v)
      } mustBe true
      val v = client.getValue("moo") getOrElse fail("get doesn't work after applyUpdate")

      v mustEqual "cow"
    }

    "work with custom maxTries" in {
      client.applyUpdate(100) { c =>
        val v = c.get("meow") getOrElse new Versioned[String]("cat")
        c.put("meow", v)
      } mustBe true
      val v = client.getValue("meow") getOrElse fail("get doesn't work after applyUpdate")

      v mustEqual "cat"
    }
   }
	
	"storeClient" should {
		"implement a mutable map" in {
			client + ("mutable" -> new Versioned("map"))
			val v = client("mutable") 
			v.getValue mustEqual "map"
			client - "mutable"
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