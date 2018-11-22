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

import java.io.IOException
import org.jsoup.Jsoup
import org.jsoup.select.Elements
import scala.collection.mutable
import scala.Option

/** Extracts links from a webpage given the HTML content (using Jsoup). */
object ExtractLinks {

  /** Extract links.
    *
    * @param src the src link
    * @param html the content from which links are to be extracted
    * @param base an optional base URI
    * @return a sequence of (source, target, anchortext).
    */
  def apply(src: String, html: String, base: String = ""): Seq[(String, String, String)] = {
    val srcMaybe: Option[String] = Option(src)
    val htmlMaybe: Option[String] = Option(html)
    val output = mutable.MutableList[(String, String, String)]()
    srcMaybe match {
      case Some(valid_src) =>
        htmlMaybe match {
          case Some (valid_html) =>
            val doc = Jsoup.parse(valid_html)
            val links: Elements = doc.select("a[href]")
            val it = links.iterator()
            while (it.hasNext) {
              val link = it.next()
              if (base.nonEmpty) link.setBaseUri(base)
              val target = link.attr("abs:href")
              if (target.nonEmpty) {
                output += ((valid_src, target, link.text))
              }
            }
          case None =>
            // do nothing
          }
      case None =>
        // do nothing
      }
    output
  }
}
