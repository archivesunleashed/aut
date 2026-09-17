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

import org.apache.tika.langdetect.optimaize.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;

/** Detects language using Apache Tika. */
object DetectLanguage {

  // OptimaizeLangDetector.loadModels() parses n-gram language profiles
  // from the classpath on construction, which is expensive to repeat on
  // every call. LanguageDetector instances are meant to be reset and
  // reused across detect() calls, but aren't documented as safe to share
  // across threads, so each thread gets its own cached instance instead
  // of either a bare shared singleton or a fresh instance (and model
  // reload) per call.
  private val detector: ThreadLocal[LanguageDetector] =
    ThreadLocal.withInitial(() => new OptimaizeLangDetector().loadModels())

  /**
   * Detects the language of a String input.
   *
   * @param input
   *   the string for which language can be detected
   * @return
   *   ISO 639-2 language code (eg. "en", "fr" or "it").
   */
  def apply(input: String): String = {
    if (input.isEmpty) {
      ""
    } else {
      val result: LanguageResult = detector.get().detect(input)
      result.getLanguage()
    }
  }
}
