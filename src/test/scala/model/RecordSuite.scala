package model

import model.DRecord.DValue
import model.Checksum._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Unit tests for the Record and Checksum algorithms.
  * Created by edniescior on 12/7/16.
  */
@RunWith(classOf[JUnitRunner])
class RecordSuite extends FunSuite {

  trait TestRecords {
    val values1 = List((NilAttribute, "first"), (NilAttribute, "second"), (StringAttribute("THIRD"), "third"))

    val rec1 = DRecord(id = "id record 1", entity = "entity A", values = Seq.empty[DValue])
    val rec2 = DRecord(id = "id record 2", entity = "entity B", values = Seq.empty[DValue])
    val rec3 = DRecord(id = "id record 3", entity = "entity C", created = Some(45599999L),
      updated = Some(345679L), action = Some(Insert), values = values1)
  }

  test("constructors working as expected") {
    new TestRecords {
      // primary
      rec1.id should ===("id record 1")
      rec1.entity should ===("entity A")
      rec1.created should be(None)
      rec1.updated should be(None)
      rec1.action should be(None)

      // apply id + entity
      rec2.id should ===("id record 2")
      rec2.entity should ===("entity B")
      rec2.created should be(None)
      rec2.updated should be(None)
      rec2.action should be(None)
    }
  }

  test("Initial record returns empty values") {
    new TestRecords {
      rec1.values should have size 0
      rec1.values should be('empty)
    }
  }

  test("calling the concatenate checksum on an empty values list returns an empty string") {
    new TestRecords {
      rec1.checksum(catChecksum) should ===("")
    }

  }

  test("calling the concatenate checksum on a list of values returns the expected result") {
    new TestRecords {
      rec3.checksum(catChecksum) should ===("firstsecondthird")
      //println(rec3.checksum(catChecksum))
    }
  }

  test("calling the md5 checksum on an empty list of values returns the MD5 sum of an empty string") {
    new TestRecords {
      rec1.checksum(md5Checksum) should ===("d41d8cd98f00b204e9800998ecf8427e") // the MD5 hash of an empty string
    }
  }

  test("calling the md5 checksum on a list of values returns the expected result") {
    new TestRecords {
      rec3.checksum(md5Checksum) should ===("272bfa314ea293c357dd9f45ba979a16")
    }
  }

  test("calling the SHA1 checksum on an empty list of values returns the SHA1 sum of an empty string") {
    new TestRecords {
      rec1.checksum(sha1Checksum) should ===("da39a3ee5e6b4b0d3255bfef95601890afd80709") // the SHA1 hash of an empty string
    }
  }

  test("calling the SHA1 checksum on a list of values returns the expected result") {
    new TestRecords {
      rec3.checksum(sha1Checksum) should ===("ba8d9a43f818557b6f52e269b44c402f16d7b226")
    }
  }

  test("calling the SHA256 checksum on an empty list of values returns the SHA256 sum of an empty string") {
    new TestRecords {
      rec1.checksum(sha256Checksum) should ===("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") // the SHA256 hash of an empty string
    }
  }

  test("calling the SHA256 checksum on a list of values returns the expected result") {
    new TestRecords {
      rec3.checksum(sha256Checksum) should ===("e3d4c0477520a72cc8b8964a863330c8558c8e436b58386829bec0f9f2723241")
    }
  }
}
