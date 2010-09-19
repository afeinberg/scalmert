package voldemort.scalmert.client

import voldemort.client.{StoreClient => JStoreClient}
import voldemort.client.UpdateAction
import voldemort.versioning.{Version, Versioned}
import voldemort.cluster.Node
import scala.collection.Map
import scala.collection.mutable.{Map => MutableMap}

/**
 * @author alex
 */
 
class StoreClient[K, V](client: JStoreClient[K, V]) { self =>

  import scalaj.collection.Imports._
  def getValue(key: K): Option[V] = Option(client.getValue(key))

  def apply(key: K) = get(key).getOrElse(throw new NoSuchElementException("No element with key: " + key))

  def get(key: K): Option[Versioned[V]] = Option(client.get(key))

  def -=(key: K) = {
    delete(key)
    this
  }

  def update(key: K, versioned: Versioned[V]): Unit = client.put(key, versioned)

  def update(key: K, value: V): Unit = client.put(key, value)

  def +=(key: K, versioned: Versioned[V]) = {
    update(key, versioned)
    this
  }

  def +=(key: K, value: V) = {
    update(key, value)
    this
  }
  
  def getAll(keys: Iterable[K]): Map[K, Versioned[V]] =
    client.getAll(keys.asJava).asScala

  def putIfNotObsolete(key: K, versioned: Versioned[V]): Boolean =
    client.putIfNotObsolete(key, versioned)

  def applyUpdate(updateFn: StoreClient[K, V] => Unit): Boolean = {
    client.applyUpdate(new UpdateAction[K, V] {
      def update(storeClient: JStoreClient[K, V]) = updateFn(self)
    })
  }

  def applyUpdate(maxTries: Int)(updateFn: StoreClient[K, V] => Unit,
                                 rollbackFn: => Unit = {}): Boolean = {
    client.applyUpdate(new UpdateAction[K, V] {
      def update(storeClient: JStoreClient[K, V]) = updateFn(self)

      override def rollback() = rollbackFn
    }, maxTries)
  }

  def delete(key: K): Boolean = client.delete(key)

  def delete(key: K, version: Version): Boolean = client.delete(key, version)

  def getResponsibleNodes(key: K): Seq[Node] =
    client.getResponsibleNodes(key).asScala

  def asMap: MutableMap[K, Versioned[V]] = new MutableMap[K, Versioned[V]] {
    override def iterator: Iterator[(K, Versioned[V])] =
      throw new UnsupportedOperationException("can't iterate over a voldemort store")

    def get(key: K): Option[Versioned[V]] = self.get(key)

    def -=(key: K) = {
      self -= (key)
      this
    }

    def +=(kv: (K, Versioned[V])) = {
      self += (kv._1, kv._2)
      this
    }
  }
}