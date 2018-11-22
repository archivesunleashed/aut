/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source toolkit for analyzing web archives.
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
// scalastyle:off underscore.import
import io.archivesunleashed.df._
import org.apache.spark.sql.functions._
// scalastyle:on underscore.import
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

  test("count records") {
    val df = RecordLoader.loadArchives(arcPath, sc)
      .extractValidPagesDF()

    // We need this in order to use the $-notation
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val results = df.select(ExtractBaseDomain($"Url").as("Domain"))
      .groupBy("Domain").count().orderBy(desc("count")).head(3)

    // Results should be:
    // +------------------+-----+
    // |            Domain|count|
    // +------------------+-----+
    // |   www.archive.org|  132|
    // |     deadlists.com|    2|
    // |www.hideout.com.br|    1|
    // +------------------+-----+

    assert(results(0).get(0) == "www.archive.org")
    assert(results(0).get(1) == 132)

    assert(results(1).get(0) == "deadlists.com")
    assert(results(1).get(1) == 2)

    assert(results(2).get(0) == "www.hideout.com.br")
    assert(results(2).get(1) == 1)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
