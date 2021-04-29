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

package io.archivesunleashed.app

import com.google.common.io.Resources
import io.archivesunleashed.RecordLoader
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class PDFInformationExtractorTest extends FunSuite with BeforeAndAfter {
  private val arcPath =
    Resources.getResource("warc/example.pdf.warc.gz").getPath
  private var sc: SparkContext = _
  private val master = "local[4]"
  private val appName = "example-spark"

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true")
    sc = new SparkContext(conf)
  }

  test("PDF information extractor DF") {
    val df = RecordLoader.loadArchives(arcPath, sc).pdfs()
    val dfResults = PDFInformationExtractor(df).collect()
    val RESULTSLENGTH = 2

    assert(dfResults.length == RESULTSLENGTH)
    assert(dfResults(0).get(0) == "20190812")
    assert(
      dfResults(0).get(
        1
      ) == "https://yorkspace.library.yorku.ca/xmlui/bitstream/handle/10315/36158/cost-analysis.pdf?sequence=1&isAllowed=y"
    )
    assert(dfResults(0).get(2) == "cost-analysis.pdf")
    assert(dfResults(0).get(3) == "pdf")
    assert(dfResults(0).get(4) == "application/pdf")
    assert(dfResults(0).get(5) == "application/pdf")
    assert(dfResults(0).get(6) == "aaba59d2287afd40c996488a39bbc0dd")
    assert(dfResults(0).get(7) == "569c28e0e8faa6945d6ca88fcd9e195825052c71")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
