package database

import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner


/**
  * Unit tests associated with connectivity to Cassandra.
  *
  * NOTE: Most of these tests require a local Cassandra node to be running (cassandra://0.0.0.0:32774).
  * See the DOCKER markdown file in Github for details about getting Cassandra running locally.
  *
  * Created by edniescior on 12/30/16.
  */
@RunWith(classOf[JUnitRunner])
class CassandraConnectionURISuite extends FunSuite with Matchers {

  /*
   * URI Object tests.
   */

  test("Cassandra connection URI object should parse an URI with a single host") {
    val uri = CassandraConnectionUri("cassandra://localhost:9042/test")
    uri.host should ===("localhost")
    uri.hosts should be(Seq("localhost"))
    uri.port should be(9042)
    uri.keyspace.get should be("test")
  }

  test("Cassandra connection URI object should parse an URI with multiple hosts") {
    val uri = CassandraConnectionUri("cassandra://localhost:9042/test?" +
      "host=otherhost.example.net&" +
      "host=yet.anotherhost.example.com")
    uri.host should ===("localhost")
    uri.hosts should contain allOf("localhost", "otherhost.example.net", "yet.anotherhost.example.com")
    uri.port should be(9042)
    uri.keyspace.get should be("test")
  }

  test("Cassandra connection URI object should parse a URI with no keyspace") {
    val uri = CassandraConnectionUri("cassandra://localhost:9042")
    uri.keyspace should be(None)
  }

  test("Cassandra connection URI object should parse a URI with root as the keyspace, effectively no keyspace") {
    val uri = CassandraConnectionUri("cassandra://localhost:9042/")
    uri.keyspace should be(None)
  }


}
