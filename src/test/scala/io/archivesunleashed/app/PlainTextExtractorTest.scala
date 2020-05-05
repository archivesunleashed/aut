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
class PlainTextExtractorTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("warc/example.warc.gz").getPath
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

  test("Plain text extractor") {
    val df = RecordLoader.loadArchives(arcPath, sc).webpages()
    val dfResults = PlainTextExtractor(df).collect()
    val RESULTSLENGTH = 94

    assert(dfResults.length == RESULTSLENGTH)
    assert(dfResults(0).get(0) == "")
    assert(dfResults(4).get(0)
      .toString
      .startsWith("Author: Spivak, John L. (John Louis), b. 1897 Published: 1939"))
    assert(dfResults(50).get(0)
      .toString
      .startsWith("How many hours in a day They tell me 24 "))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
