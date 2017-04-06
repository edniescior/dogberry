package database


import com.datastax.spark.connector.CassandraRow
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraRDD

/**
  * Implements functions for querying Cassandra tables using Spark. Both
  * relational and EAV-style tables can be queried, with basic filtering
  * of rows and columns. No joins. Everything comes back as a String.
  *
  * The results are returned an RDD of tuples. The first (String) value in
  * the tuple is the key, or unique identifier, for a record. The second value
  * in the tuple is a sequence of tuples (String, String) representing key/value
  * pairs: The first value is the attribute, or column name; the second value
  * is the value itself.
  *
  * For example:
  * (1683,List(((movie_title,The Next Three Days), (budget,35000000), (color,Color), ...))
  *
  * Created by edniescior on 3/24/17.
  */
object CassandraSparkDatabase {

  /**
    * Converts an Iterable[CassandraRow] returned by the Spark-Cassandra driver into a
    * sequence of String tuples.
    */
  type FieldParser = Iterable[CassandraRow] => Seq[(String, String)]

  /**
    * Converts an Iterable[CassandraRow] returned by the Spark-Cassandra driver into a
    * sequence of String tuples. The source table must be in relational format, i.e. there
    * is only a single record in the table per key and all data for the record resides in
    * the same row. In this case, the column name value from Cassandra becomes the attribute
    * name in the tuple. The value is the value stored under that column name.
    *
    * @param i an iterable collection of CassandraRow objects
    * @return a sequence of (String, String) tuples representing key/value (or more precisely,
    *         column name/value) pairs for the entire record
    */
  def relationalFieldParser(i: Iterable[CassandraRow]): Seq[(String, String)] = {
    /*
     * A call to cassandraTable() returns this for a relational style table:
     * (key1,CompactBuffer(CassandraRow{key: key1, attr: foo, value: 1}))
     * The CompactBuffer is what is passed in to this function. Note that
     * there is only a single CassandraRow object in the buffer.
     */

    val fields = for {
      row <- i
      columnName <- row.metaData.columnNames
    } yield (columnName, row.getString(columnName))
    fields.toSeq
  }

  /**
    * Query a Cassandra table and return the results as an RDD of tuples. In this type of
    * table, all attributes and values belonging to a single logical record reside in a single
    * Cassandra row. The first (string) value in the tuple is the unique identifier for the logical record.
    * The second value in the tuple is a sequence of tuples representing the 'Attribute' and 'Value'
    * (key/value) pairs: The first value is the attribute; the second is the value itself.
    *
    * Note: If the source table is an EAV-style table, call queryEAVTable instead.
    *
    * @param sc          the SparkContext to execute under
    * @param keySpace    the keyspace the source table is in
    * @param table       the table name of the source table
    * @param keyColumn   the column that contains the unique identifier for the logical record
    * @param columns     an optional sequence of column names to filter on. For efficiency, only select
    *                    columns that you need. Throws a NoSuchElementException if a column name does not
    *                    exist.
    * @param predicates  an optional sequence of filter predicates
    * @param fieldParser the parser needed to convert the iterable collection of CassandraRow objects
    *                    to a sequence of (String, String) tuples. The default is the relationalFieldParser.
    * @return an RDD of tuples. The first (String) value in the tuple is the key, or unique identifier,
    *         for a record. The second value in the tuple is a sequence of tuples (String, String)
    *         representing key/value pairs
    */
  def queryTable(sc: SparkContext,
                 keySpace: String,
                 table: String,
                 keyColumn: String,
                 columns: Seq[String] = Seq.empty,
                 predicates: Seq[Predicate] = Seq.empty,
                 fieldParser: FieldParser = relationalFieldParser): RDD[(String, Seq[(String, String)])] = {
    require(keySpace != null, "keySpace cannot be null in CassandraSparkDatabase.queryTable")
    require(table != null, "table cannot be null in CassandraSparkDatabase.queryTable")
    require(keyColumn != null, "keyColumn cannot be null in CassandraSparkDatabase.queryTable")

    /* apply selects */
    def applySelects(cs: Seq[String],
                     rdd: CassandraRDD[CassandraRow]): CassandraRDD[CassandraRow] = cs match {
      case Nil => rdd
      case _ => rdd.select(cs.map(col => ColumnName(col)): _*)
    }

    /* apply a where filter for each predicate */
    def applyPredicates(ps: Seq[Predicate],
                        rdd: CassandraRDD[CassandraRow]): CassandraRDD[CassandraRow] = ps match {
      case Nil => rdd
      case (p :: pss) => p match {
        case WhereOp(col, op, value) => applyPredicates(pss, rdd.where(s"$col $op ?", value))
        case WhereIn(col, val1, vals@_*) => {
          applyPredicates(pss, rdd.where(s"$col IN ?", val1 +: vals))
        }
      }
    }

    /* if selecting columns, make sure the keyColumn is included in the set or the group by will fail */
    val thisColumns = if (columns.nonEmpty && !columns.contains(keyColumn)) columns :+ keyColumn else columns

    val rdd = sc.cassandraTable(keySpace, table)
    val slicedRDD = applySelects(thisColumns, rdd)
    val filteredRDD = applyPredicates(predicates, slicedRDD)
    val groupedRDD = filteredRDD.groupBy(row => row.getString(keyColumn))
    groupedRDD.map { case (key, iter) => (key, fieldParser(iter)) }
  }

  /**
    * Query an EAV-style table and return the results as an RDD of tuples. The first (string) value in
    * the tuple is the 'Entity' identifier, i.e. the unique id of the logical record used to group the
    * individual rows of the EAV record. The second value in the tuple is a sequence of tuples
    * representing the 'Attribute' and 'Value' pairs: The first value is the attribute; the second is the
    * value.
    *
    * @param sc         the SparkContext to execute under
    * @param keySpace   the keyspace the source table is in
    * @param table      the table name of the source table
    * @param keyColumn  the column that contains the unique identifier for the logical record: The
    *                   'E' part of EAV.
    * @param attrColumn the name of the column that contains the attribute name for a single field
    *                   in the logical record: The 'A' part of EAV.
    * @param valColumn  the name of the column that contains the value for a single field in the
    *                   logical record: The 'V' part of EAV
    * @param predicates an optional sequence of filter predicates
    * @return an RDD of tuples. The first (String) value in the tuple is the key, or unique identifier,
    *         for a record. The second value in the tuple is a sequence of tuples (String, String)
    *         representing key/value pairs
    */
  def queryEAVTable(sc: SparkContext,
                    keySpace: String,
                    table: String,
                    keyColumn: String,
                    attrColumn: String,
                    valColumn: String,
                    predicates: Seq[Predicate] = Seq.empty): RDD[(String, Seq[(String, String)])] = {
    require(attrColumn != null, "attrColumn cannot be null in CassandraSparkDatabase.queryEAVTable")
    require(valColumn != null, "valColumn cannot be null in CassandraSparkDatabase.queryEAVTable")

    /**
      * Converts an Iterable[CassandraRow] returned by the Spark-Cassandra driver into a
      * sequence of String tuples. The source table must be in EAV format, i.e. a single
      * logical record is spread over any number of rows in the source table. In this case,
      * we take the value stored under the column identified by the attrColumn parameter to
      * be the attribute name in the tuple. The value is the value stored under the column
      * identified by the valColumn parameter. Any other columns are ignored.
      *
      * @param i an iterable collection of CassandraRow objects
      * @return a sequence of (String, String) tuples representing key/value (or more precisely,
      *         attribute name/value) pairs for the entire record
      */
    def eavFieldParser(i: Iterable[CassandraRow]): Seq[(String, String)] = {
      /*
       * A call to cassandraTable() returns this for a EAV style table:
       *  (key9,CompactBuffer(
       *    CassandraRow{key: key9, attr: de, value: 3},
       *    CassandraRow{key: key9, attr: fa, value: 4},
       *    CassandraRow{key: key9, attr: la, value: 6}, ...}))
       * The CompactBuffer is what is passed in to this function. Note that
       * there are multiple CassandraRow objects in the buffer.
       */

      val fields = for {
        row <- i
      } yield (row.getString(attrColumn), row.getString(valColumn))
      fields.toSeq
    }

    // pass on the call to the standard queryTable function, but provide it with the eav parser.
    // note we don't need to define columns (other than the keyColumn) as the parser already has
    // what it needs: attrColumn and valColumn.
    queryTable(sc, keySpace, table, keyColumn, predicates = predicates, fieldParser = eavFieldParser)
  }
}
