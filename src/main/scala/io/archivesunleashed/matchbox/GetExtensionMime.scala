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

import org.apache.commons.io.FilenameUtils

/** Get file extension using MIME type, then URL extension. */
// scalastyle:off object.name
object GetExtensionMime {
// scalastyle:on object.name
  /** Returns the extension of a file specified by URL
    *
    * @param url string
    * @param mimeType string
    * @return string
    */
  def apply(url: String, mimeType: String): String = {
    val tikaExtensions = DetectMimeTypeTika.getExtensions(mimeType)
    var ext = "unknown"
    // Tika method
    if (tikaExtensions.size == 1) {
      ext = tikaExtensions(0).substring(1)
    } else {
      // FilenameUtils method
      val urlExt = FilenameUtils.getExtension(url)
      if (urlExt != null && !urlExt.isEmpty) {
        // Reconcile Tika list and FilenameUtils extension
        if (tikaExtensions.size > 1) {
          if (tikaExtensions.contains("." + urlExt)) {
            ext = urlExt
          } else {
            ext = tikaExtensions(0).substring(1)
          }
        } else { // tikaExtensions.size == 0 && urlExt exists
          ext = urlExt
        }
      } // else => unknown
    }
    ext
  }
}
