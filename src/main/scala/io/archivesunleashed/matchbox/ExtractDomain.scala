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
    val maybeHost: Option[URL] = checkUrl(url)
    val maybeSource: Option[URL] = checkUrl(source)
    maybeHost match {
      case Some(host) =>
        host.getHost

      case None =>
        maybeSource match {
          case Some(source) =>
            source.getHost
          case None =>
            ""
      }
    }
  }

  def checkUrl(url: String): Option[URL] = {
    try {
      Some(new URL(url.replace("\\", "/")))
    } catch {
      case e: Exception =>
        None
    }
  }
}
