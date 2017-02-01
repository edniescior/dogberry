package database

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner
import com.datastax.spark.connector._
import java.util.UUID.randomUUID

import com.datastax.driver.core.{Cluster, ConsistencyLevel, QueryOptions}


/**
  * Unit tests associated with connectivity to Cassandra using Spark.
  *
  * NOTE: Most of these tests require a local Cassandra node to be running (cassandra://0.0.0.0:32774).
  * See the DOCKER markdown file in Github for details about getting Cassandra running locally.
  *
  * Created by edniescior on 1/27/17.
  */
@RunWith(classOf[JUnitRunner])
class CassandraSparkSuite extends FlatSpec with Matchers with BeforeAndAfter {

  /*
   * This is the connection string to the test Cassandra instance (your Docker container).
   */
  val uri = CassandraConnectionUri("cassandra://0.0.0.0:32774/test")

  /* the test Cassandra cluster */
  private val cluster = new Cluster.Builder().
    addContactPoints(uri.hosts.toArray: _*).
    withPort(uri.port).
    withQueryOptions(new QueryOptions().setConsistencyLevel(QueryOptions.DEFAULT_CONSISTENCY_LEVEL)).build

  /**
    * Perform setup on the C* instance (create keyspace and test table) and load with test data.
    */
  before {
    val session = cluster.connect(uri.keyspace.get)
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = " +
      "{'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS test.kv(key text PRIMARY KEY, value int)")
    session.execute("INSERT INTO test.kv(key, value) VALUES ('key1', 1);")
    session.execute("INSERT INTO test.kv(key, value) VALUES ('key2', 2);")
    session.execute("INSERT INTO test.kv(key, value) VALUES ('key3', 3);")
    session.close()
  }

  /**
    * Perform cleanup of the test table.  Truncate seems to be much faster than drop and
    * doesn't appear to have side-effects.
    */
  after {
    val session = cluster.connect(uri.keyspace.get)
    session.execute("TRUNCATE TABLE test.kv")
    session.close()
  }


  /*
   * Only a single Spark context can run in a single VM, so we create a new one
   * and tear it down with each test.
   */
  def withSparkContext(testCode: SparkContext => Any): Unit = {
    val appName = randomUUID.toString
    val conf = new SparkConf(true)
      .setMaster("local")
      .setAppName(appName)
      .set("spark.cassandra.connection.host", "0.0.0.0")
      .set("spark.cassandra.connection.port", "32774")
    val sc = new SparkContext(conf)
    try {
      testCode(sc)
    } finally {
      sc.stop()
    }
  }


  "A Spark RDD" should "read from a Cassandra table" in withSparkContext { sc =>
    val rdd = sc.cassandraTable("test", "kv").where("key = ?", "key1")
    rdd.first.get[String]("key") should ===("key1")
    rdd.first.get[Int]("value") should ===(1)
  }

  "A Spark RDD" should "insert a new row into a Cassandra table" in withSparkContext { sc =>
    val collection = sc.parallelize(Seq(("key4", 4)))
    collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))

    val rdd = sc.cassandraTable("test", "kv").where("key = ?", "key4")
    rdd.first.get[String]("key") should ===("key4")
    rdd.first.get[Int]("value") should ===(4)
  }

  "A Spark RDD" should "update a row in a Cassandra table" in withSparkContext { sc =>
    val collection = sc.parallelize(Seq(("key2", 100)))
    collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))

    val rdd = sc.cassandraTable("test", "kv").where("key = ?", "key2")
    rdd.first.get[String]("key") should ===("key2")
    rdd.first.get[Int]("value") should ===(100)
  }

  /*
  TODO figure out how to do deletes
  "A Spark RDD" should "delete a row in a Cassandra table" in withSparkContext { sc =>
    sc.cassandraTable("test", "kv").where("key = ?", "key1")
  }
  */
}
