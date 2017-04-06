package serde

import java.nio.charset.Charset

import model._
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner

/**
  * Unit tests associated with serialization of DRecord objects.
  *
  * Created by edniescior on 3/15/17.
  */
@RunWith(classOf[JUnitRunner])
class SerdeSuite extends FlatSpec with Matchers {

  /* JSON Serializer */

  "A JSON Serializer" should "serialize an empty DRecord to a JSON object with all fields set to null" in {
    val testRec = DRecord(id = "", entity = "", values = Seq())
    val resJSon = raw"""{"id":"","entity":"","created":null,"updated":null,"action":null,"values":[]}""".stripMargin
    val resBytes = resJSon.getBytes(Charset.forName("utf-8"))

    JsonSerializer.toJson(testRec) should === (resJSon)
    JsonSerializer.serialize(testRec) should === (resBytes)
  }

  "A JSON Serializer" should "serialize a DRecord with a None action" in {
    val testRec = DRecord(id = "bob", entity = "foo", action = None, values = Seq())
    val resJSon = raw"""{"id":"bob","entity":"foo","created":null,"updated":null,"action":null,"values":[]}""".stripMargin
    val resBytes = resJSon.getBytes(Charset.forName("utf-8"))

    JsonSerializer.toJson(testRec) should === (resJSon)
    JsonSerializer.serialize(testRec) should === (resBytes)
  }

  "A JSON Serializer" should "serialize a DRecord with Some action" in {
    val testRec = DRecord(id = "bob", entity = "foo", action = Some(Insert), values = Seq())
    val resJSon = raw"""{"id":"bob","entity":"foo","created":null,"updated":null,"action":"INS","values":[]}""".stripMargin
    val resBytes = resJSon.getBytes(Charset.forName("utf-8"))

    JsonSerializer.toJson(testRec) should === (resJSon)
    JsonSerializer.serialize(testRec) should === (resBytes)
  }

  "A JSON Serializer" should "serialize a DRecord with a None created timestamp" in {
    val testRec = DRecord(id = "bob", entity = "foo", created = None, action = Some(Update), values = Seq())
    val resJSon = raw"""{"id":"bob","entity":"foo","created":null,"updated":null,"action":"UPD","values":[]}""".stripMargin
    val resBytes = resJSon.getBytes(Charset.forName("utf-8"))

    JsonSerializer.toJson(testRec) should === (resJSon)
    JsonSerializer.serialize(testRec) should === (resBytes)
  }

  "A JSON Serializer" should "serialize a DRecord with Some created timestamp" in {
    val testRec = DRecord(id = "bob", entity = "foo", created = Some(10987L), action = Some(Update), values = Seq())
    val resJSon = raw"""{"id":"bob","entity":"foo","created":10987,"updated":null,"action":"UPD","values":[]}""".stripMargin
    val resBytes = resJSon.getBytes(Charset.forName("utf-8"))

    JsonSerializer.toJson(testRec) should === (resJSon)
    JsonSerializer.serialize(testRec) should === (resBytes)
  }

  "A JSON Serializer" should "serialize a DRecord with a None updated timestamp" in {
    val testRec = DRecord(id = "bob", entity = "foo", created = Some(10987L), updated = None, action = Some(Update),
      values = Seq())
    val resJSon = raw"""{"id":"bob","entity":"foo","created":10987,"updated":null,"action":"UPD","values":[]}""".stripMargin
    val resBytes = resJSon.getBytes(Charset.forName("utf-8"))

    JsonSerializer.toJson(testRec) should === (resJSon)
    JsonSerializer.serialize(testRec) should === (resBytes)
  }

  "A JSON Serializer" should "serialize a DRecord with Some updated timestamp" in {
    val testRec = DRecord(id = "bob", entity = "foo", created = Some(10987L), updated = Some(12345L), action = Some(Delete),
      values = Seq())
    val resJSon = raw"""{"id":"bob","entity":"foo","created":10987,"updated":12345,"action":"DEL","values":[]}""".stripMargin
    val resBytes = resJSon.getBytes(Charset.forName("utf-8"))

    JsonSerializer.toJson(testRec) should === (resJSon)
    JsonSerializer.serialize(testRec) should === (resBytes)
  }

  "A JSON Serializer" should "serialize a DRecord with a single value in the values list" in {
    val testRec = DRecord(id = "bob", entity = "foo", created = Some(10987L), updated = Some(12345L), action = Some(Delete),
      values = List((StringAttribute("foo"), "bar")))
    val resJSon =
      raw"""{"id":"bob","entity":"foo","created":10987,"updated":12345,"action":"DEL","values":[{"attr":"foo","value":"bar"}]}""".stripMargin
    val resBytes = resJSon.getBytes(Charset.forName("utf-8"))

    JsonSerializer.toJson(testRec) should === (resJSon)
    JsonSerializer.serialize(testRec) should === (resBytes)
  }

  "A JSON Serializer" should "serialize a DRecord with multiple values in the values list" in {
    val testRec = DRecord(id = "bob", entity = "foo", created = Some(10987L), updated = Some(12345L), action = Some(Delete),
      values = List((StringAttribute("foo"), "bar"), (StringAttribute("nigel"), "2"), (StringAttribute("baz"), "yah")))
    val resJSon =
      raw"""{"id":"bob","entity":"foo","created":10987,"updated":12345,"action":"DEL","values":[{"attr":"foo","value":"bar"},{"attr":"nigel","value":"2"},{"attr":"baz","value":"yah"}]}""".stripMargin
    val resBytes = resJSon.getBytes(Charset.forName("utf-8"))

    JsonSerializer.toJson(testRec) should === (resJSon)
    JsonSerializer.serialize(testRec) should === (resBytes)
  }

}
