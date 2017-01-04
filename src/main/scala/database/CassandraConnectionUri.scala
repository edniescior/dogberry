package database

import java.net.URI

/**
  * Created by edniescior on 12/29/16.
  */
case class CassandraConnectionUri(connectionString: String) {

  private val uri = new URI(connectionString)

  private val additionalHosts = Option(uri.getQuery) match {
    case Some(query) => query.split('&').map(_.split('=')).filter(param => param(0) == "host").map(param => param(1)).toSeq
    case None => Seq.empty
  }

  val host: String = uri.getHost
  val hosts: Seq[String] = Seq(uri.getHost) ++ additionalHosts
  val port: Int = uri.getPort

  /**
    * The keyspace can be passed in as part of the connect string as the 'path' element of the URI. The path is
    * anything that appears after the initial '/' following the port number.  For example, the 'test' keyspace is
    * passed in as 'cassandra://somehost:9042/test'.
    *
    * Note: The value for keyspace will be None if the path is empty or
    * contains just a single '/'. That is, 'cassandra://somehost:9042/' and 'cassandra://somehost:9042' will both
    * return None for the keyspace.
    */
  val keyspace: Option[String] =
    if (uri.getPath == ("") || uri.getPath == ("/")) None else Some(uri.getPath.substring(1))
}
