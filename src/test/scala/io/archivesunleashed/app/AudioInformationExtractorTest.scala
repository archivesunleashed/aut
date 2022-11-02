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
class AudioInformationExtractorTest extends FunSuite with BeforeAndAfter {
  private val arcPath =
    Resources.getResource("warc/example.media.warc.gz").getPath
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

  test("Audio information extractor DF") {
    val df = RecordLoader.loadArchives(arcPath, sc).audio()
    val dfResults = AudioInformationExtractor(df).collect()
    val RESULTSLENGTH = 1

    assert(dfResults.length == RESULTSLENGTH)
    assert(dfResults(0).get(0) == "20190817230242")
    assert(dfResults(0).get(1) == "20111026005826")
    assert(dfResults(0).get(2) == "https://ruebot.net/files/feniz.mp3")
    assert(dfResults(0).get(3) == "feniz.mp3")
    assert(dfResults(0).get(4) == "mp3")
    assert(dfResults(0).get(5) == "audio/mpeg")
    assert(dfResults(0).get(6) == "audio/mpeg")
    assert(dfResults(0).get(7) == "f7e7ec84b12c294e19af1ba41732c733")
    assert(dfResults(0).get(8) == "a3eb95dbbea76460529d0d9ebdde5faabaff544a")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
