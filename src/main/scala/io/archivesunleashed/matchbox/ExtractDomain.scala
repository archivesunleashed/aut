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

import io.lemonlabs.uri.Url
import io.lemonlabs.uri.config.UriConfig
import io.lemonlabs.uri.decoding.PercentDecoder
import java.net.URL

/** Extracts the host domain name from a full url string. */
object ExtractDomain {

  implicit val c: UriConfig = UriConfig(
    decoder = PercentDecoder(ignoreInvalidPercentEncoding = true)
  )

  /** Extract source domains from a full url string.
    *
    * @param url a url as a string
    * @return domain host, source or null if url is null.
    */
  def apply(url: String): String = {
    val maybeUri: Option[URL] = checkUrl(url)
    maybeUri match {
      case Some(uri) =>
        try {
          Url.parse(uri.toString).apexDomain.mkString
        } catch {
          case e: Exception =>
            ""
        }
      case None =>
        ""
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
