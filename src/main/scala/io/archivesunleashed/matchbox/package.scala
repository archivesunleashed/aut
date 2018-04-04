package io.archivesunleashed

import java.io.IOException
import java.security.MessageDigest

import scala.xml.Utility._

/**
  * Created by jimmylin on 4/4/18.
  */
package object matchbox {
  implicit class WWWLink(s: String) {
    def removePrefixWWW(): String = {
      if (s == null) return null
      s.replaceAll("^\\s*www\\.", "")
    }

    def escapeInvalidXML(): String = {
      try {
        return escape(s)
      }
      catch {
        case e: Exception => throw new IOException("Caught exception processing input row ", e)
      }
    }

    def computeHash(): String = {
      val md5 = MessageDigest.getInstance("MD5")
      return md5.digest(s.getBytes).map("%02x".format(_)).mkString
    }
  }
}
