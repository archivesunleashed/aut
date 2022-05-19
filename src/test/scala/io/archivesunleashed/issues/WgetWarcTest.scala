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
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class WgetWarcTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("warc/issue-514.warc").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  test("Test for issue 514 - wget warcs") {
    val df = RecordLoader.loadArchives(arcPath, sc).all()

    // We need this in order to use the $-notation
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val dfResults = df
      .select($"crawl_date", $"url")
      .head(2)
    val RESULTSLENGTH = 2

    assert(dfResults.length == RESULTSLENGTH)
    assert(dfResults(0).get(0) == "20210511181400")
    assert(dfResults(0).get(1) == "http://www.archiveteam.org/")
    assert(dfResults(1).get(0) == "20210511181401")
    assert(dfResults(1).get(1) == "https://wiki.archiveteam.org/")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
