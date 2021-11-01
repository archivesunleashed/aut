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

import java.net.URL

/** Extracts the host domain name from a full url string. */
object ExtractDomain {
  lazy val Suffixes: Set[String] = {
    val source = scala.io.Source
      .fromURL(
        "https://publicsuffix.org/list/public_suffix_list.dat",
        "utf-8"
      )
    try {
      source.getLines
        .map(_.trim)
        .filter(_.nonEmpty)
        .filter(!_.startsWith("//"))
        .toSet
    } catch {
      case _: Exception =>
        Set.empty
    } finally {
      source.close()
    }
  }

  /** Extract source domains from a full url string.
    *
    * @param url a url as a string
    * @return domain host, source or null if url is null.
    */
  def apply(url: String): String = {

    val maybeUrl: Option[URL] = checkUrl(url)

    maybeUrl match {

      case Some(url) =>
        val host = url.getHost.mkString
        resolve(host)
      case None =>
        ""
    }
  }

  def resolve(host: String): String = resolve(host, Suffixes)

  def resolve(host: String, suffixes: Set[String]): String = {
    val hostSplit = host.split('.')
    hostSplit.tails
      .filter(_.length > 1)
      .find { domain =>
        val suffix = domain.tail
        suffixes.contains(suffix.mkString(".")) || (suffix.length > 1 && {
          suffixes.contains("*." + suffix.tail.mkString("."))
        })
      }
      .getOrElse(hostSplit)
      .mkString(".")
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
