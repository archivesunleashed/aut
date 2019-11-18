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

package io.archivesunleashed

import com.google.common.io.Resources
import org.apache.spark.sql.SparkSession
// scalastyle:off underscore.import
import io.archivesunleashed.df._
import org.apache.spark.sql.functions._
// scalastyle:on underscore.import
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class ExtractPDFDetailsTest extends FunSuite with BeforeAndAfter {
  private val warcPath = Resources.getResource("warc/example.pdf.warc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-df"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  test("PDF DF extraction") {
    val df = RecordLoader.loadArchives(warcPath, sc)
      .pdfs()

    val extracted = df.select("url", "filename", "extension",
      "mime_type_web_server", "mime_type_tika", "md5")
      .orderBy(desc("md5")).head(2).toList
    assert(extracted.size == 2)
    assert("https://yorkspace.library.yorku.ca/xmlui/bitstream/handle/10315/36158/cost-analysis.pdf?sequence=1&isAllowed=y" == extracted(0)(0))
    assert("cost-analysis.pdf" == extracted(0)(1))
    assert("pdf" == extracted(0)(2))
    assert("application/pdf" == extracted(0)(3))
    assert("application/pdf" == extracted(0)(4))
    assert("aaba59d2287afd40c996488a39bbc0dd" == extracted(0)(5))
    assert("https://yorkspace.library.yorku.ca/xmlui/bitstream/handle/10315/36158/JCDL%20-%20Cost%20of%20a%20WARC%20Presentation-4.pdf?sequence=3&isAllowed=y" == extracted(1)(0))
    assert("JCDL%20-%20Cost%20of%20a%20WARC%20Presentation-4.pdf" == extracted(1)(1))
    assert("pdf" == extracted(1)(2))
    assert("application/pdf" == extracted(1)(3))
    assert("application/pdf" == extracted(1)(4))
    assert("322cd5239141408c42f7441f15eed9af" == extracted(1)(5))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
