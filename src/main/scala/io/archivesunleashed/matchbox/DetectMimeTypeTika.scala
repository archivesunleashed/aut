/*
 * Copyright © 2017 The Archives Unleashed Project
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
package io.archivesunleashed.matchbox

// scalastyle:off underscore.import
import scala.collection.JavaConverters._
// scalastyle:on underscore.import
import org.apache.tika.detect.DefaultDetector
import org.apache.tika.io.TikaInputStream
import org.apache.tika.mime.MimeTypes
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.Tika

/** Detect MIME type using Apache Tika. */
object DetectMimeTypeTika {
  val detector = new DefaultDetector()
  val parser = new AutoDetectParser(detector)
  val tika = new Tika(detector, parser)

  val allMimeTypes = MimeTypes.getDefaultMimeTypes();

  /** Detect MIME type from an input string.
    *
    * @param content a byte array of content for which to detect the MimeType
    * @return MIME type (e.g. "text/html" or "application/xml") or "N/A".
    */
  def apply(content: Array[Byte]): String = {
    if (content.size == 0) {
      "N/A"
    } else {
      val tis = TikaInputStream.get(content)
      val mimetype = tika.detect(tis)
      tis.close()
      mimetype
    }
  }

  /** Return the best guess at a file extension from a MIME type string
    *
    * @param mimeType string representation of the MimeType
    * @return file extension (e.g. ".jpg" for "image/jpeg").
    */
  def getExtension(mimeType: String): String = {
    val regMimeType = allMimeTypes.forName(mimeType)
    try {
      regMimeType.getExtension
    } catch {
      case e: Exception => ""
    }
  }

  /** Return the list of all known file extensions for a MIME type string
    *
    * @param mimeType string representation of the MimeType
    * @return list of file extensions (e.g. ".jpg" for "image/jpeg").
    */
  def getExtensions(mimeType: String): List[String] = {
    val regMimeType = allMimeTypes.forName(mimeType)
    try {
      regMimeType.getExtensions.asScala.toList
    } catch {
      case e: Exception => Nil
    }
  }
}
