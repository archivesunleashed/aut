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

import java.io.IOException
import org.jsoup.Jsoup

/** Remove HTML markup with JSoup. */
object RemoveHTML {

  /** Remove HTML markup.
   *
   * @param content an html or text string
   * @return content without html markup.
   */
  def apply(content: String) = {
    try {
      Jsoup.parse(content).text().replaceAll("[\\r\\n]+", " ")
    }
    catch {
      case e: Exception => throw new IOException("Caught exception processing input row ", e)
    }
  }
}
