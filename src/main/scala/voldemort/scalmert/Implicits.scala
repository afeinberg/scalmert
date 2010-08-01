package voldemort.scalmert

/**
 * @author alex
 */

import voldemort.client.{StoreClient => JStoreClient}

object Implicits {
  implicit def enrichStoreClient[K, V](client: JStoreClient[K, V]): StoreClient[K, V] =
    new StoreClient[K, V](client)
}