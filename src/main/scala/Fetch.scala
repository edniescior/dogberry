import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import java.util.UUID.randomUUID

import com.typesafe.scalalogging.LazyLogging
import database.{CassandraSparkDatabase, Op, WhereOp}
import model.DRecord
import org.apache.hadoop.io.compress.GzipCodec
import serde.JsonSerializer


/**
  * Created by edniescior on 2/28/17.
  */
object Fetch extends LazyLogging {


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
    logger.info("Executing query")
    val res = CassandraSparkDatabase.queryEAVTable(sc, "test", "eav",
      keyColumn = "ent",
      attrColumn = "attr",
      valColumn = "value",
      predicates = Seq(WhereOp("ent", Op.EQ, "1683")))
    logger.info("Query complete")
    //res.foreach(println(_))

    /* Convert into an RDD of DRecord objects - use the default values for created and updated field names. */
    val dRec = res.map((row) => DRecord(id = row._1, entity = "EAV", fields = row._2,
      createdAttrLabel = "created", updatedAttrLabel = "updated"))
    logger.info("DRecords created")
    //dRec foreach(println(_))

    /* Convert to JSON */
    val dRecJson = dRec map (JsonSerializer.toJson(_))
    logger.info("DRecords serialized")
    //dRecJson foreach (println(_))

    /* Write out to file */
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val dateStamp = dateFormat.format(Calendar.getInstance().getTime)
    val outputPath = "/Users/eniesc200/Work/repos/dogberry/tmp/" + dateStamp

    logger.info("Writing to " + outputPath)
    dRecJson.saveAsTextFile(outputPath, classOf[GzipCodec])
    logger.info("Write complete")

    sc.stop()

  }
}
