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

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class DetectLanguageTest extends FunSuite {
  val english =
    "This is a sample piece of English text used to detect language."
  val french =
    "Ceci est un exemple de texte en français utilisé pour détecter la langue."
  val german =
    "Dies ist ein Beispieltext auf Deutsch, der zur Spracherkennung dient."

  test("Detects language from a string") {
    assert(DetectLanguage("") == "")
    assert(DetectLanguage(english) == "en")
  }

  // DetectLanguage caches one LanguageDetector per thread (see
  // DetectLanguage.scala) instead of constructing a new one, and
  // reloading its language models, on every call. This confirms that a
  // single cached detector instance produces correct results across many
  // sequential detect() calls on the same thread, and stays correct when
  // many threads are each reusing their own cached instance concurrently.
  test(
    "Cached, reused ThreadLocal detector produces correct, stable results under concurrent load") {
    val samples: Seq[(String, String)] =
      Seq(english -> "en", french -> "fr", german -> "de")

    implicit val ec: ExecutionContext = ExecutionContext.global
    val iterationsPerSample = 100

    val futures = for {
      (input, expected) <- samples
      _ <- 1 to iterationsPerSample
    } yield Future {
      val detected = DetectLanguage(input)
      assert(detected == expected, s"expected $expected but got $detected under concurrent load")
    }

    Await.result(Future.sequence(futures), 2.minutes)
  }
}
