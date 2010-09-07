package voldemort.scalmert.client

import scala.collection.mutable.Map
import org.specs.Specification
import voldemort.serialization.StringSerializer
import voldemort.client.MockStoreClientFactory
import voldemort.versioning.{VectorClock, Versioned => JVersioned}

import voldemort.scalmert.Implicits._
import voldemort.scalmert.versioning._

/**
  *  @author alex
  */

class  StoreClientFactorySpec extends Specification {
  val serializer = new StringSerializer
  val factory:StoreClientFactory = new MockStoreClientFactory(serializer, serializer)    
  val client:StoreClient[String, String] = factory.getStoreClient[String, String]("test")
  
  def getClock(nodes: List[Int]): VectorClock = {
    nodes.foldLeft(new VectorClock) {
      (clock, node) =>
        clock.incremented(node.asInstanceOf[Short], System.currentTimeMillis)
    }
  }

  "factory" should {
    "return a valid StoreClient" in {
      client + ("hello" -> Versioned("world"))
      client("hello").getValue mustEqual "world"
    }
  }

  "setting a custom inconsistency resolver" should {
    "be supported" in {
      val resolver: Seq[JVersioned[String]] => Seq[JVersioned[String]] = { results =>
        val versions = results.map(_.getVersion)
        val mergedVersion = versions.foldLeft(new VectorClock) { 
          (clock, ver) =>
            clock.merge(ver.asInstanceOf[VectorClock])
        }
        val value = (results.map(_.getValue).foldLeft(new StringBuffer) {
          (sb, str) =>
            sb.append(str)
        }).toString

        Seq[JVersioned[String]](Versioned(value, mergedVersion))
      }

      val c:StoreClient[String, String] = factory.getStoreClient[String, String]("test", resolver)
      c + ("quux" -> Versioned("foo", getClock(List(0))))
      c + ("quux" -> Versioned("bar", getClock(List(1))))
      c("quux").getValue mustEqual "foobar"
    }
  }
}
