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

import de.l3s.boilerpipe.extractors.DefaultExtractor
import java.io.IOException

/** Extract raw text content from an HTML page, minus "boilerplate" content (using boilerpipe).  */
object ExtractBoilerpipeText {
  /** Uses boilerpipe to extract raw text content from a page.
   *
   * ExtractBoilerpipeText removes boilerplate text (e.g. a copyright statement) from an HTML string.
   *
   * @param input an html string possibly containing boilerpipe text
   * @return text with boilerplate removed or Nil if the text is empty.
   */
  def apply(input: String) = {
    try {
      if (input.isEmpty) Nil
      else extract(input)
    } catch {
      case e: Exception =>
        throw new IOException("Caught exception processing input row " + e)
    }
  }

  /** Extracts boilerplate.
   *
   * @param input an html string possibly containing boilerpipe text
   * @return filtered text or Nil if the text is empty.
   */
  def extract (input: String) = {
    val text = DefaultExtractor.INSTANCE.getText(input).replaceAll("[\\r\\n]+", " ").trim()
    if (text.isEmpty) Nil
    else text
  }
}
