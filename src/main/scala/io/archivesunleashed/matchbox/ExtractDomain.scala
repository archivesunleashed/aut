/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source platform for analyzing web archives.
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

import java.net.URL

/** Extracts the host domain name from a full url string. */
object ExtractDomain {
  /** Extract source domains from a full url string.
   *
   * @param url a url as a string
   * @param source an optional default url for urls with no valid domain host
   * @return domain host, source or null if url is null.
   */
  def apply(url: String, source: String = ""): String = {
    if (url == null) return null
    var host: String = null
    try {
      host = new URL(url).getHost
    } catch {
      case e: Exception => // it's okay
    }
    if (host != null || source == null) return host
    try {
      new URL(source).getHost
    } catch {
      case e: Exception => null
    }
  }
}
