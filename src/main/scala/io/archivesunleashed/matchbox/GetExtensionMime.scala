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

import org.apache.commons.io.FilenameUtils

/** Get file extension using MIME type, then URL extension. */
object GetExtensionMime {

  /** Returns the extension of a file specified by URL
    *
    * @param url string
    * @param mimeType string
    * @return string
    */
  def apply(url: String, mimeType: String): String = {
    val tikaExtensions = DetectMimeTypeTika.getExtensions(mimeType)
    var ext = "unknown"
    // Determine extension from MIME type
    if (tikaExtensions.size == 1) {
      ext = tikaExtensions(0).substring(1)
    } else {
      // Get extension from URL
      val urlExt = FilenameUtils.getExtension(url).toLowerCase
      if (urlExt != null) {
        // Reconcile MIME-based and URL extension, preferring MIME-based
        if (tikaExtensions.size > 1) {
          if (tikaExtensions.contains("." + urlExt)) {
            ext = urlExt
          } else {
            ext = tikaExtensions(0).substring(1)
          }
        } else { // tikaExtensions.size == 0
          if (!urlExt.isEmpty) {
            ext = urlExt
          } // urlExt = "" => ext = "unknown"
        }
      }
    }
    ext
  }
}
