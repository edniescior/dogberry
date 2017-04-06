import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import java.util.UUID.randomUUID

import database.{CassandraSparkDatabase, Op, WhereOp}
import model.{DRecord, StringAttribute}
import serde.JsonSerializer


/**
  * Created by edniescior on 2/28/17.
  */
object Fetch {


  def main(args: Array[String]): Unit = {

    /* construct the spark context */
    val appName = randomUUID.toString
    val conf = new SparkConf(true)
      .setMaster("local")
      .setAppName(appName)
      .set("spark.cassandra.connection.host", "0.0.0.0")
      .set("spark.cassandra.connection.port", "32774")
    val sc = new SparkContext(conf)

    /* Fetch the data */
    val res = CassandraSparkDatabase.queryEAVTable(sc, "test", "eav",
      keyColumn = "ent",
      attrColumn = "attr",
      valColumn = "value",
      predicates = Seq(WhereOp("ent", Op.EQ, "1683")))
    res.foreach(println(_))
    println("Fetched records")

    /* Convert into an RDD of DRecord objects - use the default values for created and updated field names. */
    val dRec = res.map((row) => DRecord(id = row._1, entity = "EAV", fields = row._2,
      createdAttrLabel = "created", updatedAttrLabel = "updated"))
    //dRec foreach(println(_))

    /* Convert to JSON */
    val dRecJson = dRec map (JsonSerializer.toJson(_))

    /* Write out to file */
    dRecJson foreach (println(_))

    sc.stop()

  }


}
