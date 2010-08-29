package voldemort.scalmert.client

import java.lang.String
import voldemort.cluster.failuredetector.FailureDetector
import voldemort.store.Store
import voldemort.client.{StoreClient => JStoreClient, StoreClientFactory => JStoreClientFactory}
import voldemort.versioning.{Versioned, InconsistencyResolver}
import java.util.{List => JList}

/**
 * @author alex
 */

class StoreClientFactory(inner: JStoreClientFactory) extends JStoreClientFactory {
  def getFailureDetector: FailureDetector = inner.getFailureDetector

  def close: Unit = {}

  def getRawStore[K, V](storeName: String, resolver: InconsistencyResolver[Versioned[V]]): Store[K, V] =
    inner.getRawStore[K, V](storeName, resolver)

  def getStoreClient[K, V](storeName: String, resolver: InconsistencyResolver[Versioned[V]]): JStoreClient[K, V] =
    inner.getStoreClient[K, V](storeName, resolver)

  def getStoreClient[K, V](storeName: String): JStoreClient[K, V] = inner.getStoreClient[K, V](storeName)
  
  import scalaj.collection.Imports._
  def getStoreClient[K, V](storeName: String, resolver: Seq[Versioned[V]] => Seq[Versioned[V]]): JStoreClient[K, V]= {
    val ir:InconsistencyResolver[Versioned[V]] = new InconsistencyResolver[Versioned[V]] {
      def resolveConflicts(items: JList[Versioned[V]]): JList[Versioned[V]] = resolver(items.asScala).asJava
    }
    getStoreClient(storeName, ir)
  }
}