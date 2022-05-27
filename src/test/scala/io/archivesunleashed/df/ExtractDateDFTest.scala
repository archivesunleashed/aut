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
import io.archivesunleashed.udfs.{
  extractDate,
  extractDomain,
  extractLinks,
  removePrefixWWW
}
import org.apache.spark.sql.functions.{array, explode_outer, lower, udf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class ExtractDateDFTest extends FunSuite with BeforeAndAfter {
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

  test("Extract dates YYYY DF") {
    val df = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .keepValidPagesDF()

    val dest = udf((vs: Seq[Any]) => vs(0).toString.split(",")(1))

    // We need this in order to use the $-notation
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    import org.apache.spark.sql.functions._
    // scalastyle:on

    val interResults = df
      .select(
        $"domain",
        $"url",
        extractDate($"crawl_date", lit("YYYY")).as("crawl_date"),
        explode_outer(extractLinks($"url", $"content")).as("link")
      )
      .filter(
        lower($"content").contains("keynote")
      )

    val results = interResults
      .select(
        $"url",
        $"domain",
        $"crawl_date",
        dest(array($"link")).as("destination_page")
      )
      .head(3)

    assert(results(0).get(0) == "http://www.archive.org/index.php")
    assert(results(0).get(1) == "archive.org")
    assert(results(0).get(2) == "2008")
    assert(results(0).get(3) == "http://www.archive.org/")

    assert(results(1).get(0) == "http://www.archive.org/index.php")
    assert(results(1).get(1) == "archive.org")
    assert(results(1).get(2) == "2008")
    assert(
      results(1).get(
        3
      ) == "http://www.archive.org/web/web.php"
    )

    assert(results(2).get(0) == "http://www.archive.org/index.php")
    assert(results(2).get(1) == "archive.org")
    assert(results(2).get(2) == "2008")
    assert(results(2).get(3) == "http://www.archive.org/details/movies")
  }

  test("Extract dates YYYYMM DF") {
    val df = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .keepValidPagesDF()

    val dest = udf((vs: Seq[Any]) => vs(0).toString.split(",")(1))

    // We need this in order to use the $-notation
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    import org.apache.spark.sql.functions._
    // scalastyle:on

    val interResults = df
      .select(
        $"domain",
        $"url",
        extractDate($"crawl_date", lit("YYYYMM")).as("crawl_date"),
        explode_outer(extractLinks($"url", $"content")).as("link")
      )
      .filter(
        lower($"content").contains("keynote")
      ) // filtered on keyword internet

    val results = interResults
      .select(
        $"url",
        $"domain",
        $"crawl_date",
        dest(array($"link")).as("destination_page")
      )
      .head(3)

    assert(results(0).get(0) == "http://www.archive.org/index.php")
    assert(results(0).get(1) == "archive.org")
    assert(results(0).get(2) == "200804")
    assert(results(0).get(3) == "http://www.archive.org/")

    assert(results(1).get(0) == "http://www.archive.org/index.php")
    assert(results(1).get(1) == "archive.org")
    assert(results(1).get(2) == "200804")
    assert(
      results(1).get(
        3
      ) == "http://www.archive.org/web/web.php"
    )

    assert(results(2).get(0) == "http://www.archive.org/index.php")
    assert(results(2).get(1) == "archive.org")
    assert(results(2).get(2) == "200804")
    assert(results(2).get(3) == "http://www.archive.org/details/movies")
  }

  test("Extract dates MM DF") {
    val df = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .keepValidPagesDF()

    val dest = udf((vs: Seq[Any]) => vs(0).toString.split(",")(1))

    // We need this in order to use the $-notation
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    import org.apache.spark.sql.functions._
    // scalastyle:on

    val interResults = df
      .select(
        $"domain",
        $"url",
        extractDate($"crawl_date", lit("MM")).as("crawl_date"),
        explode_outer(extractLinks($"url", $"content")).as("link")
      )
      .filter(
        lower($"content").contains("keynote")
      )

    val results = interResults
      .select(
        $"url",
        $"domain",
        $"crawl_date",
        dest(array($"link")).as("destination_page")
      )
      .head(3)

    assert(results(0).get(0) == "http://www.archive.org/index.php")
    assert(results(0).get(1) == "archive.org")
    assert(results(0).get(2) == "04")
    assert(results(0).get(3) == "http://www.archive.org/")

    assert(results(1).get(0) == "http://www.archive.org/index.php")
    assert(results(1).get(1) == "archive.org")
    assert(results(1).get(2) == "04")
    assert(
      results(1).get(
        3
      ) == "http://www.archive.org/web/web.php"
    )

    assert(results(2).get(0) == "http://www.archive.org/index.php")
    assert(results(2).get(1) == "archive.org")
    assert(results(2).get(2) == "04")
    assert(results(2).get(3) == "http://www.archive.org/details/movies")
  }

  test("Extract dates DD DF") {
    val df = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .keepValidPagesDF()

    val dest = udf((vs: Seq[Any]) => vs(0).toString.split(",")(1))

    // We need this in order to use the $-notation
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    import org.apache.spark.sql.functions._
    // scalastyle:on

    val interResults = df
      .select(
        $"domain",
        $"url",
        extractDate($"crawl_date", lit("DD")).as("crawl_date"),
        explode_outer(extractLinks($"url", $"content")).as("link")
      )
      .filter(
        lower($"content").contains("keynote")
      )

    val results = interResults
      .select(
        $"url",
        $"domain",
        $"crawl_date",
        dest(array($"link")).as("destination_page")
      )
      .head(3)

    assert(results(0).get(0) == "http://www.archive.org/index.php")
    assert(results(0).get(1) == "archive.org")
    assert(results(0).get(2) == "30")
    assert(results(0).get(3) == "http://www.archive.org/")

    assert(results(1).get(0) == "http://www.archive.org/index.php")
    assert(results(1).get(1) == "archive.org")
    assert(results(1).get(2) == "30")
    assert(
      results(1).get(
        3
      ) == "http://www.archive.org/web/web.php"
    )

    assert(results(2).get(0) == "http://www.archive.org/index.php")
    assert(results(2).get(1) == "archive.org")
    assert(results(2).get(2) == "30")
    assert(results(2).get(3) == "http://www.archive.org/details/movies")
  }

  test("Extract dates YYYYMMDD DF") {
    val df = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .keepValidPagesDF()

    val dest = udf((vs: Seq[Any]) => vs(0).toString.split(",")(1))

    // We need this in order to use the $-notation
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    import org.apache.spark.sql.functions._
    // scalastyle:on

    val interResults = df
      .select(
        $"domain",
        $"url",
        extractDate($"crawl_date", lit("YYYYMMDDHHMMSS")).as("crawl_date"),
        explode_outer(extractLinks($"url", $"content")).as("link")
      )
      .filter(
        lower($"content").contains("keynote")
      )

    val results = interResults
      .select(
        $"url",
        $"domain",
        $"crawl_date",
        dest(array($"link")).as("destination_page")
      )
      .head(3)

    assert(results(0).get(0) == "http://www.archive.org/index.php")
    assert(results(0).get(1) == "archive.org")
    assert(results(0).get(2) == "20080430204826")
    assert(results(0).get(3) == "http://www.archive.org/")

    assert(results(1).get(0) == "http://www.archive.org/index.php")
    assert(results(1).get(1) == "archive.org")
    assert(results(1).get(2) == "20080430204826")
    assert(
      results(1).get(
        3
      ) == "http://www.archive.org/web/web.php"
    )

    assert(results(2).get(0) == "http://www.archive.org/index.php")
    assert(results(2).get(1) == "archive.org")
    assert(results(2).get(2) == "20080430204826")
    assert(results(2).get(3) == "http://www.archive.org/details/movies")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
