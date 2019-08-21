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

import org.apache.tika.parser.pdf.PDFParser
import org.junit.runner.RunWith
import org.scalatest.FunSuite
// scalastyle:off underscore.import
import org.scalatest.Matchers
// scalastyle:on underscore.import
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractTextFromPDFsTest extends FunSuite with Matchers {
  test("get parser") {
    ExtractTextFromPDFs.pdfParser shouldBe a[PDFParser]
  }
}
