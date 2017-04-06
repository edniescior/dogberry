package model

import model.DRecord.DValue


/**
  * Provides a Value Object for an individual data record. Fields within the record are stored in a sequence.
  * This ordering is important to create a valid checksum for the record. The Value Object will also return a
  * checksum based on the values in the fields and on the algorithm provided.
  *
  * @param id      An unique identifier for this record, e.g. a primary key or UUID. It must be defined.
  * @param entity  The entity this record belongs to, e.g. the table name or column family. It must be defined.
  * @param created The epoch value of when this record was created.
  * @param updated The epoch value of when this record was last updated.
  * @param action  The changed-data-capture action indicator, i.e. Insert, Update or Delete.
  * @param values  A sequence representing the fields in the record. The fields are in the format
  *                of a tuple (Attribute, Value)
  *
  *                Created by edniescior on 11/23/16.
  */
case class DRecord(id: String,
                   entity: String,
                   created: Option[Long] = None,
                   updated: Option[Long] = None,
                   action: Option[Action] = None,
                   values: Seq[DValue] = Seq.empty) {
  require(id != null, "id cannot be null in DRecord")
  require(entity != null, "entity cannot be null in DRecord")

  /**
    * Generate a checksum of this record based on the values stored within.
    *
    * @param f the algorithm to generate the required checksum.
    * @return the checksum value for this record.
    */
  def checksum(f: (Seq[(Attribute, String)]) => String): String = f(values)

  /**
    * Get the value tuple for a given string attribute. If an attribute is repeated in the values collection, then the
    * first match is returned. None is returned if there is no match. None is returned for any non-string attribute
    * including the NilAttribute.
    *
    * @param attr the Attribute to return from the values collection
    * @return the first occurrence of a match with the given attribute in the values collection
    */
  def getValueByAttribute(attr: Attribute): Option[DValue] = {
    def find(xs: Seq[DValue], attr: StringAttribute): Option[DValue] = xs match {
      case Nil => None
      case ((x: StringAttribute, y: String) :: xs) =>
        if (x.label == attr.label) Some(x, y) else find(xs, attr)
      case _ => find(xs.tail, attr)
    }

    attr match {
      case NilAttribute => None
      case s: StringAttribute => find(values, s)
      case _ => None
    }
  }
}


object DRecord {

  /**
    * Defines a field in record as being made up of a tuple consisting of an attribute label and a string value.
    * Right now we just handle all values as strings.
    */
  //  TODO Change to accept any object as a value, not just strings.
  type DValue = (Attribute, String)

  /**
    * Overloaded constructor to build DRecord objects out of a collection of key/value pairs. If the created
    * and updated timestamps are members of the collection, calling this method will extract them and update
    * the DRecord accordingly. If the timestamps cannot be found in the collection, or the values cannot be
    * converted to a Long, then they default to None(Long).
    *
    * @param id               An unique identifier for this record, e.g. a primary key or UUID.
    * @param entity           The entity this record belongs to, e.g. the table name or column family.
    * @param fields           a collection of key/value pairs that make up the fields in the record
    * @param createdAttrLabel if the created timestamp is a field in the record, this is the key to find the value
    * @param updatedAttrLabel if the updated timestamp is a field in the record, this is the key to find the value
    * @return a populated DRecord
    */
  def apply(id: String,
            entity: String,
            fields: Seq[(String, String)],
            createdAttrLabel: String,
            updatedAttrLabel: String): DRecord = {
    // convert a String to Long safely
    def toLong(s: String): Option[Long] = {
      try {
        Some(s.toLong)
      } catch {
        case e: NumberFormatException => None
      }
    }

    // extract the timestamp values for created and updated
    def getTimestamp(fName: String, fs: Seq[(String, String)]): Option[Long] = fs match {
      case Nil => None
      case (x :: xs) => {
        if (x._1 == fName) toLong(x._2)
        else getTimestamp(fName, xs)
      }
      case _ =>  None
    }

    val created = getTimestamp(createdAttrLabel, fields)
    val updated = getTimestamp(updatedAttrLabel, fields)
    val values = for {(fName, fVal) <- fields} yield (StringAttribute(fName), fVal)
    DRecord(id = id, entity = entity, created = created, updated = updated, values = values)
  }

}


/**
  * Captures the types of attribute (i.e. label/field name) that can appear within a field tuple (DValue).
  */
sealed trait Attribute

/** An empty label - no label, in effect. */
case object NilAttribute extends Attribute

/**
  * A string attribute label (i.e. header/field name).
  *
  * @param label The attribute label for this field. Uniqueness isn't enforced,
  *              but one should make them unique for usability's sake.
  */
case class StringAttribute(label: String) extends Attribute

/**
  * Captures the types of changed-data-capture states that a record can have: Insert, Update or Delete.
  */
sealed trait Action {
  val label: String
}

/** The record has been identified as a new insert. */
case object Insert extends Action {
  override val label: String = "INS"
}

/** The record has been identified as an update to an existing record. */
case object Update extends Action {
  override val label: String = "UPD"
}

/** The record has been marked as deleted. */
case object Delete extends Action {
  override val label: String = "DEL"
}



