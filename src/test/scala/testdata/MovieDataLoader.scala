package testdata

import java.io._

import scala._
import scala.collection.mutable.ListBuffer

/**
  * A script to load the IMDB data taken from Kaggle (CSV) and transform it into CQL insert statements.
  * The target tables are be structured differently. One is in the EAV format; the other looks like
  * a regular relational table.
  *
  * Beware. The paths are hard-coded. There's no error checking. This is just a utility object that won't be used
  * again unless we need to re-generate the test data files again.
  *
  * Created by edniescior on 2/3/17.
  */
object MovieDataLoader {

  /**
    * Escape CQL special chars.
    *
    * @param inStr the string with special chars
    * @return the string with special chars escaped
    */
  def escape(inStr: String): String = {
    //val ESCAPE_CHARS = List("+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", "~", "*", "?", ":")
    val ESCAPE_CHARS = List("{", "}")
    def escapeChar(str: String, chrs: List[String]): String = chrs match {
      case (Nil) => str
      case (c :: cs) => escapeChar(str.replace(c, "\\\\" + c), cs)
    }

    escapeChar(inStr.trim, ESCAPE_CHARS)
      .replaceAll("'", "''")  // ' is escaped differently from the rest
      .replaceAll("\\P{Print}", "")  // strip out non printable chars
  }

  /**
    * Read in the data source CSV file. A header is expected.
    *
    * @param path the path to the CSV file
    * @return a List of records. Each record is a map where the key is the column heading.
    */
  def loadFile(path: String): List[Map[String, String]] = {
    /* we need to fudge the timestamp values so we generate them randomly */
    val r = new scala.util.Random(100)
    val createdT = System.currentTimeMillis() // use this as the created timestamp

    val bufferedSource = io.Source.fromFile(path)

    var header = List[String]()
    val records = ListBuffer[Map[String, String]]()
    for ((line, count) <- bufferedSource.getLines.zipWithIndex) {
      val cols = line.split(",").map(escape(_))

      if (count == 0) {
        // deal with the header, adding the extra column headers we need
        header = cols.toList :+ "created" :+ "updated" :+ "key"
      } else {
        // add the extra column values and combine with headers
        val thisCol = cols.toList :+ createdT.toString :+ (createdT + r.nextInt(10000000)).toString :+ count.toString
        records += (header zip thisCol).toMap
      }
    }
    bufferedSource.close
    records.toList
  }

  /**
    * Given a record, format it into a group of insert statements for an EAV style table.
    * Null values are filtered out.
    *
    * @param record a map representing a record where the key is the column heading.
    * @return a string of insert statements (separated by a newline char).
    */
  def formatToEAV(record: Map[String, String]): String = {
    // subtract 1 for key, which we don't need inserted
    val fields = new Array[String](record.size - 1)
    val key = record("key")
    var i = 0
    for ((k, v) <- record if k != "key" && v != "") {
      fields(i) = (s"INSERT INTO test.eav(ent, attr, value) VALUES ('${key}', '${k}', '${v}');")
      i = i + 1
    }
    fields.slice(0, i).mkString("\n") // slice to strip out the nulls
  }

  /**
    * Given a record, format it into a group of insert statements for a relational style table. Null
    * values are filtered out, i.e. no explicit insert of 'null'.
    * @param record a map representing a record where the key is the column heading.
    * @return a map representing a record where the key is the column heading.
    */
  def formatToRelational(record: Map[String, String]): String = {
    def isNumeric(str:String): Boolean = str.matches("[-+]?\\d+(\\.\\d+)?")
    val filteredRecord = record.filter(r => r._2 != "")
    val headers = filteredRecord.keys.mkString("(", ",", ")")
    val escapedVals = filteredRecord.values.map(v => if (isNumeric(v)) v else "'" + v +"'")
    val values = escapedVals.mkString("(", ",", ")")
    s"INSERT INTO test.rel${headers} VALUES ${values};"
  }


  def main(args: Array[String]): Unit = {
    val rowLimit = 10000000;  // limit the num of rows output

    // load the source data
    val records = loadFile("./src/test/resources/data/movie_metadata.csv")
    println(s"Loaded ${records.size} records.")

    val pw = new PrintWriter(new File("./src/test/resources/data/insert_eav_test.cql"), "UTF-8")

    // print the EAV data
    var count = 0
    pw.println("\n-- EAV Test Data --\n\nTRUNCATE TABLE test.eav;\n")
    for (record <- records if count < rowLimit) {
      pw.println(formatToEAV(record))
      count = count + 1
    }
    println(s"Wrote ${count} EAV records.")

    // print the relational data
    count = 0
    pw.println("\n-- Relational Test Data --\n\nTRUNCATE TABLE test.rel;\n")
    for (record <- records if count < rowLimit) { // limit the num of rows if needed
      pw.println(formatToRelational(record))
      count = count + 1
    }
    println(s"Wrote ${count} relational records.")

    pw.close()

  }


}
