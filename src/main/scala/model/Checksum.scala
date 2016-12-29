package model

import model.DRecord.DValue
import java.security.MessageDigest

/**
  * This object encapsulates different common checksum algorithms. They all operate on a
  * sequence of DRecord fields to return a checksum value.
  *
  * Created by eniesc200 on 12/19/16.
  */
object Checksum {

  private val md5digest = MessageDigest.getInstance("MD5")
  private val sha1digest = MessageDigest.getInstance("SHA1")
  private val sha256digest = MessageDigest.getInstance("SHA-256")

  /**
    * A function that returns the values as a single concatenated string.
    *
    * @return a hashing function that operates on a sequence of values to return a concatenated string.
    */
  def catChecksum: (Seq[DValue] => String) = (s: Seq[DValue]) => s.foldLeft[String]("")((a, b) => a + b._2)

  /**
    * A function that returns an MD5 hash of the concatenated string of values.
    *
    * @return a hashing function that operates on a sequence of values to return an MD5 checksum.
    */
  def md5Checksum: (Seq[DValue] => String) = (s: Seq[DValue]) => digest(md5digest)(s)

  /**
    * A function that returns a SHA-1 hash of the concatenated string of values.
    *
    * @return a hashing function that operates on a sequence of values to return an SHA-1 checksum.
    */
  def sha1Checksum: (Seq[DValue] => String) = (s: Seq[DValue]) => digest(sha1digest)(s)

  /**
    * A function that returns a SHA-256 hash of the concatenated string of values.
    *
    * @return a hashing function that operates on a sequence of values to return an SHA-256 checksum.
    */
  def sha256Checksum: (Seq[DValue] => String) = (s: Seq[DValue]) => digest(sha256digest)(s)

  /*
   * A helper function that concatenates all the values into a single UTF-8 string
   * and passes it through the appropriate MessageDigest object to generate a checksum.
   */
  private def digest(digest: MessageDigest)(s: Seq[DValue]): String = {
    val textBytes = catChecksum(s).getBytes("UTF-8")
    digest.digest(textBytes).map("%02x".format(_)).mkString
  }
}
