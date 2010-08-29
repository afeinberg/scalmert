package voldemort.scalmert

/**
 * @author alex
 */

import client._
import voldemort.client.{StoreClient => JStoreClient, StoreClientFactory => JStoreClientFactory}

object Implicits {
  implicit def enrichStoreClient[K, V](client: JStoreClient[K, V]): StoreClient[K, V] =
    new StoreClient[K, V](client)
  implicit def enrichStoreClientFactory(factory: JStoreClientFactory): StoreClientFactory =
    new StoreClientFactory(factory)
}