package database

import database.Database._
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
class CassandraSuite extends FunSuite with Matchers {

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


  /*
   * Cassandra connectivity tests.
   */

  //  TODO Figure out how to get the Cassandra dependency sorted - also finally disconnecting each test
  test("Failing to connect should throw a DatabaseException") {
    val db = new CassandraDatabase()
    val thrown = the[DatabaseException] thrownBy {
      db.connect("cassandra://0.0.0.0:999/test") // the port is wrong
    }
    thrown.getMessage should ===("Failed to connect to cassandra://0.0.0.0:999/test")
    // TODO fix this so we can check the underlying exception: thrown.getCause should be (NoHostAvailableException)
  }

  test("Connecting to a locally running Cassandra database providing a keyspace") {
    val db = new CassandraDatabase()
    val currentStatus: Status = db.connect("cassandra://0.0.0.0:32774/test")
    currentStatus shouldBe a[Connected]
  }

  test("Connecting to a locally running Cassandra database without providing a keyspace") {
    val db = new CassandraDatabase()
    val currentStatus: Status = db.connect("cassandra://0.0.0.0:32774")
    currentStatus shouldBe a[Connected]
  }

  test("Disconnecting from a Cassandra database that we haven't connected to yet") {
    val db = new CassandraDatabase()
    val currentStatus: Status = db.disconnect()
    currentStatus should be(Disconnected)
  }

  test("Disconnecting from a Cassandra database") {
    val db = new CassandraDatabase()
    val connectStatus: Status = db.connect("cassandra://0.0.0.0:32774/test")
    connectStatus shouldBe a[Connected]
    val disconnectStatus: Status = db.disconnect()
    disconnectStatus should be(Disconnected)
  }

  /*
   * Cassandra DML tests.
   */

  test("Inserting a record.") {
    ???
  }

  test("Updating a record.") {
    ???
  }
}
