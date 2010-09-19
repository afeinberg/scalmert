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
 
class StoreClient[K, V](client: JStoreClient[K, V]) extends MutableMap[K, Versioned[V]] {
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

  override def + [V1 >: Versioned[V]](kv: (K, V1)) = {
    val v = kv._2.asInstanceOf[Versioned[V]]
    put(kv._1, v)
    this.asInstanceOf[MutableMap[K, V1]]
  }

  override def -(key: K): StoreClient[K, V] = { 
    delete(key)
    this
  }
  
  override def -=(key: K) = {
    delete(key)
    this
  }

  override def +=(kv: (K, Versioned[V])) = {
    put(kv._1, kv._2)
    this
  }
  
  override def iterator: Iterator[(K, Versioned[V])] = 
    throw new UnsupportedOperationException("can't iterate over a voldemort store")

  def getAll(keys: Iterable[K]): Map[K, Versioned[V]] =
    client.getAll(keys.asJava).asScala[K, Versioned[V]]


  def put(key: K, value: V) = { 
    client.put(key, value)
  }
  
  override def put(key: K, versioned: Versioned[V]) = {
    client.put(key, versioned)
    None
  }

  def putIfNotObsolete(key: K, versioned: Versioned[V]): Boolean =
    client.putIfNotObsolete(key, versioned)

  def applyUpdate(updateFn: StoreClient[K, V] => Unit): Boolean = {
    client.applyUpdate(new UpdateAction[K, V] {
      def update(storeClient: JStoreClient[K, V]) = updateFn(StoreClient.this)
    })
  }

  def applyUpdate(maxTries: Int)(updateFn: StoreClient[K, V] => Unit,
                                 rollbackFn: => Unit = {}): Boolean = {
    client.applyUpdate(new UpdateAction[K, V] {
      def update(storeClient: JStoreClient[K, V]) = updateFn(StoreClient.this)

      override def rollback() = rollbackFn
    }, maxTries)
  }

  def delete(key: K): Boolean = client.delete(key)

  def delete(key: K, version: Version): Boolean = client.delete(key, version)

  def getResponsibleNodes(key: K): Seq[Node] =
    client.getResponsibleNodes(key).asScala
}