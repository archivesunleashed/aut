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
import io.archivesunleashed.udfs.extractDomain
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class SimpleDfTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-df"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  test("Count records DF") {
    val df = RecordLoader
      .loadArchives(arcPath, sc)
      .webpages()

    // We need this in order to use the $-notation
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val results = df
      .groupBy($"domain")
      .count()
      .sort($"count".desc)
      .head(3)

    // Results should be:
    // +------------------+-----+
    // |            domain|count|
    // +------------------+-----+
    // |       archive.org|   91|
    // |     deadlists.com|    2|
    // |    hideout.com.br|    1|
    // +------------------+-----+

    assert(results(0).get(0) == "archive.org")
    assert(results(0).get(1) == 91)

    assert(results(1).get(0) == "deadlists.com")
    assert(results(1).get(1) == 2)

    assert(results(2).get(0) == "hideout.com.br")
    assert(results(2).get(1) == 1)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
