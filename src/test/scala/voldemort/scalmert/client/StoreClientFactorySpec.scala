package voldemort.scalmert.client

import scala.collection.mutable.Map
import org.specs.Specification
import voldemort.serialization.StringSerializer
import voldemort.client.MockStoreClientFactory

import voldemort.scalmert.Implicits._
import voldemort.scalmert.versioning._

/**
  *  @author alex
  */

class  StoreClientFactorySpec extends Specification {
  val serializer = new StringSerializer
  val factory:StoreClientFactory = new MockStoreClientFactory(serializer, serializer)    
  val client:StoreClient[String, String] = factory.getStoreClient[String, String]("test")
  
  "factory" should {
    "return a valid StoreClient" in {
      client + ("hello" -> Versioned("world"))
      client("hello").getValue mustEqual "world"
    }
  }
}
