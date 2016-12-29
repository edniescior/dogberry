package model

import model.DRecord.DValue


/**
  * Provides a Value Object for an individual data record. Fields within the record are stored in a sequence.
  * This ordering is important to create a valid checksum for the record. The Value Object will also return a
  * checksum based on the values in the fields and on the algorithm provided.
  *
  * @param id An unique identifier for this record, e.g. a primary key or UUID.
  * @param entity The entity this record belongs to, e.g. the table name or column family.
  * @param created The epoch value of when this record was created.
  * @param updated The epoch value of when this record was last updated.
  * @param action The changed-data-capture action indicator, i.e. Insert, Update or Delete.
  * @param values A sequence representing the fields in the record. The fields are in the format
  *               of a tuple (Attribute, Value)
  *
  * Created by eniesc200 on 11/23/16.
  */
case class DRecord(id: String,
                   entity: String,
                   created: Option[Long] = None,
                   updated: Option[Long] = None,
                   action: Option[Action] = None,
                   values: Seq[DValue]) {

  /**
    * Generate a checksum of this record based on the values stored within.
    *
    * @param f the algorithm to generate the required checksum.
    * @return the checksum value for this record.
    */
  def checksum(f: (Seq[(Attribute, String)]) => String): String = f(values)
}


object DRecord {

  /**
    * Defines a field in record as being made up of a tuple consisting of an attribute label and a string value.
    * Right now we just handle all values as strings.
    */
  //  TODO Change to accept any object as a value, not just strings.
  type DValue = (Attribute, String)

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
  *               but one should make them unique for usability's sake.
  */
case class StringAttribute(label: String) extends Attribute

/**
  * Captures the types of changed-data-capture states that a record can have: Insert, Update or Delete.
  */
sealed trait Action

/** The record has been identified as a new insert. */
case object Insert extends Action

/** The record has been identified as an update to an existing record. */
case object Update extends Action

/** The record has been marked as deleted. */
case object Delete extends Action



