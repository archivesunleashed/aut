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

import org.apache.tika.language.LanguageIdentifier

/** Detects language using Apache Tika. */
object DetectLanguage {

  /** Detects the language of a String input.
   *
   * @param input the string for which language can be detected
   * @return ISO 639-2 language code (eg. "en", "fr" or "it").
   */
  def apply(input: String): String = {
    if (input.isEmpty) ""
    else new LanguageIdentifier(input).getLanguage
  }
}
