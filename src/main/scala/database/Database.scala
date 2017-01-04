package database

/**
  * A simplified interface to databases.
  *
  * Created by edniescior on 11/24/16.
  */
object Database {

  /**
    * An encapsulation of query result sets.
    */
  case class ResultSet(/*...*/)

  /**
    * An encapsulation of connection pools and other information.
    */
  case class Connection(/*...*/)

  /**
    * An encapsulation of database exceptions.
    *
    * @param message an error message
    * @param cause   the underlying exception
    */
  case class DatabaseException(message: String, cause: Throwable) extends
    RuntimeException(message, cause)

  /**
    * Sealed hierarchy for status 'flags'.
    */
  sealed trait Status

  case object Disconnected extends Status

  case class Connected(connection: Connection) extends Status

  case class QuerySucceeded(results: ResultSet) extends Status

  case class QueryFailed(e: DatabaseException) extends Status

}

trait Database {

  import Database._

  /**
    * Connect to the database with the given connection string.
    *
    * @param server the connection string
    * @return the returned status: Connected on success; QueryFailed on failure
    */
  def connect(server: String): Status

  def disconnect(): Status

  def query(/*...*/): Status
}
