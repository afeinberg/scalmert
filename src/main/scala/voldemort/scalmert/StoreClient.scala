package voldemort.scalmert

import voldemort.client.{StoreClient => JStoreClient}
import voldemort.client.UpdateAction
import voldemort.versioning.{Version, Versioned}
import voldemort.cluster.Node
import scala.collection.Map

/**
 * @author alex
 */

class StoreClient[K, V](client: JStoreClient[K, V]) {
  import scalaj.collection.Imports._
  def getValue(key: K): Option[V] = {
    val v = client.getValue(key)

    if (v == null)
      None
    else
      Some(v)
  }

  def get(key: K): Option[Versioned[V]] = {
    val v = client.get(key)

    if (v == null)
      None
    else
      Some(v)
  }

  def getAll(keys: Iterable[K]): Map[K, Versioned[V]] =
    client.getAll(keys.asJava).asScala[K, Versioned[V]]

  def put(key: K, value: V) = client.put(key, value)

  def put(key: K, versioned: Versioned[V]) = client.put(key, versioned)

  def putIfNotObsolete(key: K, versioned: Versioned[V]): Boolean =
    client.putIfNotObsolete(key, versioned)

  def applyUpdate(updateFn: StoreClient[K, V] => Unit): Boolean = {
    client.applyUpdate(new UpdateAction[K, V] {
      def update(storeClient: JStoreClient[K, V]) = {
        updateFn(new StoreClient(storeClient))
      }
    })
  }

  def applyUpdate(maxTries: Int)(updateFn: StoreClient[K, V] => Unit): Boolean = {
    client.applyUpdate(new UpdateAction[K, V] {
      def update(storeClient: JStoreClient[K, V]) = {
        updateFn(new StoreClient(storeClient))
      }
    }, maxTries)
  }

  def delete(key: K): Boolean = client.delete(key)

  def delete(key: K, version: Version): Boolean = client.delete(key, version)

  def getResponsibleNodes(key: K): Seq[Node] =
    client.getResponsibleNodes(key).asScala
}