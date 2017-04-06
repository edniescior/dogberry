package serde

import java.nio.charset.Charset

import model.{DRecord, StringAttribute}
import model.DRecord.DValue

/**
  * Created by edniescior on 3/14/17.
  */
trait Serializer {

  def serialize(dRecord: DRecord): Array[Byte]
}


/**
  *
  */
object JsonSerializer extends Serializer {

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
   *
   */
  private def toJsValue(dRecord: DRecord): JsValue = Json.toJson(dRecord)

  /**
    *
    * @param dRecord
    * @return
    */
  def toJson(dRecord: DRecord): String = Json.stringify(toJsValue(dRecord))

  /**
    *
    * @param dRecord
    * @return
    */
  def toPrettyJson(dRecord: DRecord): String = Json.prettyPrint(toJsValue(dRecord))

  /**
    *
    * @param dRecord
    * @return
    */
  def serialize(dRecord: DRecord): Array[Byte] = toJson(dRecord).getBytes(Charset.forName("UTF-8"))
}