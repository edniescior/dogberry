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

    session.execute("CREATE TABLE IF NOT EXISTS test.kv(key text PRIMARY KEY, value int, attr text)")
    session.execute("INSERT INTO test.kv(key, value, attr) VALUES ('key1', 1, 'foo');")
    session.execute("INSERT INTO test.kv(key, value, attr) VALUES ('key2', 2, 'bar');")
    session.execute("INSERT INTO test.kv(key, value, attr) VALUES ('key3', 3, 'zoo');")

    // EAV style
    session.execute("CREATE TABLE IF NOT EXISTS test.ka (key text, attr text, value int, PRIMARY KEY (key, attr))")
    session.execute("INSERT INTO test.ka(key, attr, value) VALUES ('key9', 'de', 3);")
    session.execute("INSERT INTO test.ka(key, attr, value) VALUES ('key9', 'fa', 4);")
    session.execute("INSERT INTO test.ka(key, attr, value) VALUES ('key9', 'so', 5);")
    session.execute("INSERT INTO test.ka(key, attr, value) VALUES ('key9', 'la', 6);")
    session.execute("INSERT INTO test.ka(key, attr, value) VALUES ('key9', 'ti', 7);")
    session.execute("INSERT INTO test.ka(key, attr, value) VALUES ('key10', 'too', 10);")
    session.execute("INSERT INTO test.ka(key, attr, value) VALUES ('key10', 'taa', 11);")
    session.execute("INSERT INTO test.ka(key, attr, value) VALUES ('key10', 'tee', 12);")
    session.execute("INSERT INTO test.ka(key, attr, value) VALUES ('key11', 'fu', 13);")
    session.execute("INSERT INTO test.ka(key, attr, value) VALUES ('key11', 'man', 14);")
    session.execute("INSERT INTO test.ka(key, attr, value) VALUES ('key11', 'chu', 15);")
    session.close()
  }

  /**
    * Perform cleanup of the test table.  Truncate seems to be much faster than drop and
    * doesn't appear to have side-effects.
    */
  after {
    val session = cluster.connect(uri.keyspace.get)
    session.execute("TRUNCATE TABLE test.kv;")
    session.execute("TRUNCATE TABLE test.ka;")
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

  /*
   * Test basic direct RDD functionality.
   */
  "A Spark RDD" should "read from a Cassandra table" in withSparkContext { sc =>
    val rdd = sc.cassandraTable("test", "kv").where("key = ?", "key1")
    rdd.first.get[String]("key") should ===("key1")
    rdd.first.get[Int]("value") should ===(1)
    rdd.first.get[String]("attr") should ===("foo")
  }

  "A Spark RDD" should "insert a new row into a Cassandra table" in withSparkContext { sc =>
    val collection = sc.parallelize(Seq(("key4", 4, "moo")))
    collection.saveToCassandra("test", "kv", SomeColumns("key", "value", "attr"))

    val rdd = sc.cassandraTable("test", "kv").where("key = ?", "key4")
    rdd.first.get[String]("key") should ===("key4")
    rdd.first.get[Int]("value") should ===(4)
    rdd.first.get[String]("attr") should ===("moo")
  }

  "A Spark RDD" should "update a row in a Cassandra table" in withSparkContext { sc =>
    val collection = sc.parallelize(Seq(("key2", 100)))
    collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))

    val rdd = sc.cassandraTable("test", "kv").where("key = ?", "key2")
    rdd.first.get[String]("key") should ===("key2")
    rdd.first.get[Int]("value") should ===(100)
    rdd.first.get[String]("attr") should ===("bar")
  }

  /*
  TODO figure out how to do deletes
  "A Spark RDD" should "delete a row in a Cassandra table" in withSparkContext { sc =>
    sc.cassandraTable("test", "kv").where("key = ?", "key1")
  }
  */


  /*
   * Test CassandraSparkDatabase
   */
  "A CassandraSparkDatabase" should "on queryTable, throw an exception if " +
    "no keyspace is given" in withSparkContext { sc =>
    an[IllegalArgumentException] should be thrownBy {
      CassandraSparkDatabase.queryTable(sc, null, "kv", "key")
    }
  }

  "A CassandraSparkDatabase" should "on queryTable, throw an exception if " +
    "no table is given" in withSparkContext { sc =>
    an[IllegalArgumentException] should be thrownBy {
      CassandraSparkDatabase.queryTable(sc, "test", null, "key")
    }
  }

  "A CassandraSparkDatabase" should "on queryTable, throw an exception if " +
    "no keyColumn is given" in withSparkContext { sc =>
    an[IllegalArgumentException] should be thrownBy {
      CassandraSparkDatabase.queryTable(sc, "test", "kv", null)
    }
  }

  /* Test selects */

  "A CassandraSparkDatabase" should "on queryTable, return all columns if no filter columns " +
    "are given" in withSparkContext { sc =>
    val rdd = CassandraSparkDatabase.queryTable(sc, "test", "kv", "key")
    val results = rdd.first._2
    results.size should ===(3)
    val colNames = results.unzip._1
    colNames should contain("key")
    colNames should contain("value")
    colNames should contain("attr")
  }

  "A CassandraSparkDatabase" should "on queryTable, return only the  column given in the columns " +
    "filter (plus the keyColumn is added implicitly)" in withSparkContext { sc =>
    val rdd = CassandraSparkDatabase.queryTable(sc, "test", "kv", "key", Seq("value"))
    val results = rdd.first._2
    results.size should ===(2) // 1 we filtered for + the keyColumn
  val colNames = results.unzip._1
    colNames should contain("key")
    colNames should contain("value")
  }

  "A CassandraSparkDatabase" should "on queryTable, return all columns given in the columns " +
    "filter (with the keyColumn added explicitly)" in withSparkContext { sc =>
    val rdd = CassandraSparkDatabase.queryTable(sc, "test", "kv", "key", Seq("key", "value"))
    val results = rdd.first._2
    results.size should ===(2) // we filtered for 2, explicitly adding the keyColumn
  val colNames = results.unzip._1
    colNames should contain("key")
    colNames should contain("value")
  }

  "A CassandraSparkDatabase" should "on queryTable, will throw an exception if a column given in the columns filter " +
    "is not a valid column name" in withSparkContext { sc =>

    an[NoSuchElementException] should be thrownBy {
      CassandraSparkDatabase.queryTable(sc, "test", "kv", "key", Seq("foob"))
    }
  }

  /* Test predicates */

  "A CassandraSparkDatabase" should "on queryTable, return all rows if no predicates " +
    "are given" in withSparkContext { sc =>
    val rdd = CassandraSparkDatabase.queryTable(sc, "test", "kv", "key")
    rdd.count() should ===(3L) // the total number of unique test records (i.e. unique keys) in the test data set
  }

  "A CassandraSparkDatabase" should "on queryTable, return only a single row given a predicate " +
    "WhereOp filter that should only bring back a single record" in withSparkContext { sc =>
    val rdd = CassandraSparkDatabase.queryTable(sc, "test", "kv", "key",
      predicates = Seq(WhereOp("key", Op.EQ, "key2")))
    rdd.count() should ===(1L)
    val (key, fields) = rdd.first()
    key should ===("key2")
    fields.size should ===(3)
    fields should contain("key", "key2")
    fields should contain("value", "2")
    fields should contain("attr", "bar")
  }

  "A CassandraSparkDatabase" should "on queryTable, return 1 rows given a collection of predicate " +
    "WhereOp filters that should only bring back 1 record" in withSparkContext { sc =>
    val rdd = CassandraSparkDatabase.queryTable(sc, "test", "kv", "key",
      predicates = Seq(WhereOp("key", Op.EQ, "key3"), WhereOp("value", Op.EQ, "3")))
    rdd.count() should ===(1L)
    val (key, fields) = rdd.first()
    key should ===("key3")
    fields.size should ===(3)
    fields should contain("key", "key3")
    fields should contain("value", "3")
    fields should contain("attr", "zoo")
  }

  "A CassandraSparkDatabase" should "on queryTable, return no rows given a collection of predicate " +
    "WhereOp filters for which there is no match" in withSparkContext { sc =>
    val rdd = CassandraSparkDatabase.queryTable(sc, "test", "kv", "key",
      predicates = Seq(WhereOp("key", Op.EQ, "key3"), WhereOp("value", Op.EQ, "0")))
    rdd.count() should ===(0)
  }

  "A CassandraSparkDatabase" should "on queryTable, return certain rows given a predicate " +
    "WhereIn filter that matches the expected rows" in withSparkContext { sc =>
    val rdd = CassandraSparkDatabase.queryTable(sc, "test", "kv", "key",
      predicates = Seq(WhereIn("key", "key1", "key3")))
    rdd.count() should ===(2)
    val results = rdd.collect()
    val keys = results.unzip._1
    keys should contain("key1")
    keys should contain("key3")
  }

  /* Test EAV-style tables */

  "A CassandraSparkDatabase" should "on queryTable, return a single complete record " +
    "given an EAV-style record spanning multiple rows" in withSparkContext { sc =>
    val rdd = CassandraSparkDatabase.queryEAVTable(sc, "test", "ka",
      keyColumn = "key",
      attrColumn = "attr",
      valColumn = "value",
      predicates = Seq(WhereOp("key", Op.EQ, "key9")))
    rdd.count() should ===(1)
    val results = rdd.take(1)
    val (keys, fields) = (results(0)._1, results(0)._2)
    keys should ===("key9")
    fields.size should ===(5)
    fields should contain("de", "3")
    fields should contain("fa", "4")
    fields should contain("so", "5")
    fields should contain("la", "6")
    fields should contain("ti", "7")
  }

  "A CassandraSparkDatabase" should "on queryTable of an EAV, return 2 rows given an keyColumn " +
    "value and a predicate that filters out all but 2 rows" in withSparkContext { sc =>
    val rdd = CassandraSparkDatabase.queryEAVTable(sc, "test", "ka",
      keyColumn = "key",
      attrColumn = "attr",
      valColumn = "value",
      predicates = Seq(WhereIn("key", "key9", "key11")))
    rdd.count() should ===(2)
    val results = rdd.collect()
    val (keys, fields) = results.unzip
    keys should contain("key9")
    keys should contain("key11")
  }
}
