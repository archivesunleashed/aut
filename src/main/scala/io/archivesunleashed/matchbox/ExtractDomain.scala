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
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/** Extracts the host domain name from a full url string. */
object ExtractDomain {
  private var suffixesBroadcast: Option[(String, Broadcast[Set[String]])] =
    None

  /**
   * Broadcasts the public suffix list once per Spark application.
   *
   * `Suffixes` fetches `publicsuffix.org`'s list lazily and caches it per-JVM, so calling
   * `ExtractDomain(url)` directly inside a Spark transformation makes every executor JVM
   * independently fetch it over the network the first time it processes a record. Broadcasting it
   * once from the driver and using the `apply(url, suffixes)` overload below avoids that.
   *
   * @param sc
   *   the active SparkContext
   * @return
   *   a broadcast variable containing the public suffix list.
   */
  def broadcast(sc: SparkContext): Broadcast[Set[String]] = synchronized {
    suffixesBroadcast match {
      case Some((appId, bc)) if appId == sc.applicationId => bc
      case _ =>
        for ((_, bc) <- suffixesBroadcast) bc.destroy()
        val bc = sc.broadcast(Suffixes)
        suffixesBroadcast = Some((sc.applicationId, bc))
        bc
    }
  }

  lazy val Suffixes: Set[String] = {
    val source = scala.io.Source
      .fromURL("https://publicsuffix.org/list/public_suffix_list.dat", "utf-8")
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

  /**
   * Extract source domains from a full url string.
   *
   * @param url
   *   a url as a string
   * @return
   *   domain host, source or null if url is null.
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

  /**
   * Extract source domains from a full url string, using a pre-computed public suffix list (e.g.
   * from `ExtractDomain.broadcast(sc).value`). Prefer this overload over the single-argument
   * `apply` inside Spark transformations to avoid a redundant network fetch per executor.
   *
   * @param url
   *   a url as a string
   * @param suffixes
   *   a public suffix list
   * @return
   *   domain host, source or "" if url is invalid.
   */
  def apply(url: String, suffixes: Set[String]): String = {
    checkUrl(url) match {
      case Some(u) => resolve(u.getHost.mkString, suffixes)
      case None => ""
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
