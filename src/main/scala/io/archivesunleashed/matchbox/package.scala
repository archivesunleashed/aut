/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source toolkit for analyzing web archives.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.archivesunleashed

import java.io.IOException
import java.security.MessageDigest
// scalastyle:off underscore.import
import scala.xml.Utility._
// scalastyle:on underscore.import


/** Package object which supplies implicits providing common UDF-related functionalities. */
package object matchbox {
  implicit class WWWLink(s: String) {
    def removePrefixWWW(): String = {
      val maybeString: Option[String] = Option(s)
      maybeString match {
        case Some(s) => s.replaceAll("^\\s*www\\.", "")
        case None => ""
      }
    }

    def escapeInvalidXML(): String = {
      try {
        escape(s)
      }
      catch {
        case e: Exception => throw new IOException("Caught exception processing input row ", e)
      }
    }

    def computeHash(): String = {
      val md5 = MessageDigest.getInstance("MD5")
      md5.digest(s.getBytes).map("%02x".format(_)).mkString
    }
  }
}
