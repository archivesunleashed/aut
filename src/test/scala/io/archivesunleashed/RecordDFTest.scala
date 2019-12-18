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
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class RecordDFTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true");
    sc = new SparkContext(conf)
  }

  test("keep Valid Pages") {
    val expected = "http://www.archive.org/"
    val base = RecordLoader.loadArchives(arcPath, sc).all()
      .keepValidPagesDF().take(1)(0)(1)
    assert (base.toString == expected)
  }

  test("Discard MimeTypes") {
    val expected = "filedesc://IAH-20080430204825-00000-blackbook.arc"
    val MimeTypes = Set("text/html")
    val base = RecordLoader.loadArchives(arcPath, sc).all()
      .discardMimeTypesDF(MimeTypes).take(1)(0)(1)

    assert (base.toString == expected)
  }

  test("Discard Date") {
    val expected = "20080430"
    val date = "20080429"
    val base = RecordLoader.loadArchives(arcPath, sc).webpages()
          .discardDateDF(date).take(1)(0)(0)

    assert (base.toString == expected)
  }

  test("Discard Urls") {
    val expected = "http://www.archive.org/index.php"
    val URls = Set("http://www.archive.org/")
    val base = RecordLoader.loadArchives(arcPath, sc).webpages()
        .discardUrlsDF(URls).take(1)(0)(1)

    assert (base.toString == expected)
  }

  test("Discard Domains") {
    val expected = "http://www.hideout.com.br/"
    val domains = Set("www.archive.org")
    val base = RecordLoader.loadArchives(arcPath, sc).webpages()
      .discardDomainsDF(domains).take(1)(0)(1)

    assert (base.toString == expected)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}