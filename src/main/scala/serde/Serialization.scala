package serde

import java.nio.charset.Charset

import model.{DRecord, StringAttribute}
import model.DRecord.DValue

/**
  * An abstraction for serializing DRecord objects to an array of bytes.
  *
  * Created by edniescior on 3/14/17.
  */
trait Serializer {

  /**
    * Serialize a DRecord into a byte array.
    * @param dRecord the record to serialize
    * @return the record represented as an array of bytes
    */
  def serialize(dRecord: DRecord): Array[Byte]
}


/**
  * Serializes a DRecord into JSON bytes (UTF-8 charset).
  */
object JsonSerializer extends Serializer {

  // Uses the Scala Play JSON library.
  import play.api.libs.json._

  implicit val dValueWrites = new Writes[DValue] {
    def writes(dValue: DValue): JsObject = Json.obj(
      "attr" -> (dValue._1 match {
        case StringAttribute(l) => l
        case _ => JsNull
      }),
      "value" -> dValue._2
    )
  }

  implicit val dRecordWrites = new Writes[DRecord] {
    def writes(dRecord: DRecord): JsObject = Json.obj(
      "id" -> dRecord.id,
      "entity" -> dRecord.entity,
      "created" -> dRecord.created,
      "updated" -> dRecord.updated,
      "action" -> {dRecord.action match {
        case None => JsNull
        case Some(b) => b.label
      }},
      "values" -> dRecord.values
    )
  }

  /*
   * Convert a DRecord into a JsValue object.
   */
  private def toJsValue(dRecord: DRecord): JsValue = Json.toJson(dRecord)

  /**
    * Convert a DRecord into a machine-friendly JSON string.
    * @param dRecord the record to convert
    * @return a machine-friendly JSON representation of the record
    */
  def toJson(dRecord: DRecord): String = Json.stringify(toJsValue(dRecord))

  /**
    * Convert a DRecord into a pretty JSON string.
    * @param dRecord the record to convert
    * @return a pretty JSON representation of the record
    */
  def toPrettyJson(dRecord: DRecord): String = Json.prettyPrint(toJsValue(dRecord))

  /**
    * Serialize a JSON representation of a DRecord into a byte array. Uses UTF-8 as the
    * character set.
    * @param dRecord the record to serialize
    * @return a byte array containing the JSON representation of the record
    */
  def serialize(dRecord: DRecord): Array[Byte] = toJson(dRecord).getBytes(Charset.forName("UTF-8"))
}