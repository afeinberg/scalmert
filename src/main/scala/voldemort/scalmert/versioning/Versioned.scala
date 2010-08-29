package voldemort.scalmert.versioning

import voldemort.versioning._

/**
 *	@author alex
 */

object Versioned {
	def apply[V](data: V): Versioned[V] = new Versioned(data)
	def apply[V](data: V, version: Version): Versioned[V] = new Versioned(data, version)
}