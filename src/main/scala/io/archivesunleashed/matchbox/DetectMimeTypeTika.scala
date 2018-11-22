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
package io.archivesunleashed.matchbox

import java.io.ByteArrayInputStream
import org.apache.tika.Tika
import org.apache.tika.detect.DefaultDetector
import org.apache.tika.parser.AutoDetectParser

/** Detect MIME type using Apache Tika. */
object DetectMimeTypeTika {

  /** Detect MIME type from an input string.
   *
   * @param content a string of content for which to detect the MimeType
   * @return MIME type (e.g. "text/html" or "application/xml") or "N/A".
   */
  def apply(content: String): String = {
    if (content.isEmpty) {
      "N/A"
    } else {
      val is = new ByteArrayInputStream(content.getBytes)
      val detector = new DefaultDetector()
      val parser = new AutoDetectParser(detector)
      val mimetype = new Tika(detector, parser).detect(is)
      mimetype
    }
  }
}
