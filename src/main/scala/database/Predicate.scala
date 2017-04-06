package database

/**
  * An abstraction for capturing SQL-like predicate (where) clauses. Right now it
  * just handles "WHERE x op value" and "WHERE x IN" type constructs.
  *
  * Example usage:
  *   val wheres = Seq(
  *       WhereIn("title_year", "2010", "2011", "2012"),
  *       WhereOp("language", EQ, "English"),
  *       WhereOp("content_rating", EQ, "PG-13"),
  *       WhereOp("gross", GT, 1000000))
  *
  * Created by edniescior on 3/31/17.
  */
sealed trait Predicate

/**
  * Operators for Where clauses.
  */
object Op extends Enumeration {
  type Op = Value

  val EQ = Value("=")
  val NE = Value("!=")
  val LTGT = Value("<>")
  val LT = Value("<")
  val LE = Value("<=")
  val GT = Value(">")
  val GE = Value(">=")
}
import Op._

/**
  * Represents a SQL "WHERE x op value" clause where +op+ is a comparison
  * operator: =, !=, <>, <, <=, >, or >=.
  * @param columnName the name of the column to compare
  * @param op the comparison operator
  * @param value the value to compare
  * @tparam T
  */
case class WhereOp[T](columnName: String, op: Op, value: T) extends Predicate

/**
  * Represents a SQL "WHERE x IN (a, b, c,...)" clause.
  * @param columnName the name of the column to compare
  * @param val1 the value to compare
  * @param vals more values.
  * @tparam T
  */
case class WhereIn[T](columnName: String, val1: T, vals: T*) extends Predicate


