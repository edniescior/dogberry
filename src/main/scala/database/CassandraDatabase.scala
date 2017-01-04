package database
import com.datastax.driver.core.{Cluster, ConsistencyLevel, QueryOptions, Session}
import database.Database._

/**
  * Created by edniescior on 12/29/16.
  */
class CassandraDatabase extends Database {

  /* a Cassandra session object */
  private var session: Option[Session] = None

  /**
    * Connect to the database with the given connection string.
    *
    * @param server the connection string
    * @return the returned status: Connected on success; QueryFailed on failure
    */
  override def connect(server: String): Status = {
    val uri = CassandraConnectionUri(server)
    val defaultConsistencyLevel: ConsistencyLevel = QueryOptions.DEFAULT_CONSISTENCY_LEVEL
    val cluster = new Cluster.Builder().
      addContactPoints(uri.hosts.toArray: _*).
      withPort(uri.port).
      withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build

    try {
      session = uri.keyspace match {
        case Some(k) => Some(cluster.connect(k))  // connect to a keyspace
        case None => Some(cluster.connect())      // connect without specifying a keyspace
      }
    } catch {
      case e: Exception => throw DatabaseException("Failed to connect to " + server, e)
    }
    Connected(Connection())
  }

  override def disconnect(): Status = {
    session match {
      case Some(ses) => ses.close()
      case None => // ignore if session is None
    }
    Disconnected
  }

  override def query(): Status = ???
}
