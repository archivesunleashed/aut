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
import org.apache.commons.io.FilenameUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class ArchiveRecordTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val warcPath = Resources.getResource("warc/example.warc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _
  private val exampleArc = "example.arc.gz"
  private val exampleWarc = "example.warc.gz"
  private val exampleDate = "20080430"
  private val exampleUrl = "www.archive.org"
  private val exampleStatusCode1 = "000"
  private val exampleStatusCode2 = "200"
  private val exampleMimeType = "text/plain"

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true");
    sc = new SparkContext(conf)
  }

  test("Count records") {
    assert(RecordLoader.loadArchives(arcPath, sc).count == 300L)
    assert(RecordLoader.loadArchives(warcPath, sc).count == 299L)
  }

  test("Resource name produces expected result") {
    val textSampleArc = RecordLoader
      .loadArchives(arcPath, sc)
      .map(x => FilenameUtils.getName(x.getArchiveFilename))
      .take(3)
    val textSampleWarc = RecordLoader
      .loadArchives(warcPath, sc)
      .map(x => FilenameUtils.getName(x.getArchiveFilename))
      .take(3)
    assert(textSampleArc.deep == Array(exampleArc, exampleArc, exampleArc).deep)
    assert(
      textSampleWarc.deep == Array(exampleWarc, exampleWarc, exampleWarc).deep
    )
  }

  test("Crawl Dates") {
    val textSampleArc = RecordLoader
      .loadArchives(arcPath, sc)
      .map(x => x.getCrawlDate)
      .take(3)
    val textSampleWarc = RecordLoader
      .loadArchives(warcPath, sc)
      .map(x => x.getCrawlDate)
      .take(3)
    assert(
      textSampleArc.deep == Array(exampleDate, exampleDate, exampleDate).deep
    )
    assert(
      textSampleWarc.deep == Array(exampleDate, exampleDate, exampleDate).deep
    )
  }

  test("Domains") {
    val textSampleArc = RecordLoader
      .loadArchives(arcPath, sc)
      .map(x => x.getDomain)
      .take(3)
    val textSampleWarc = RecordLoader
      .loadArchives(warcPath, sc)
      .map(x => x.getDomain)
      .take(3)
    assert(textSampleArc.deep == Array("", "", exampleUrl).deep)
    assert(textSampleWarc.deep == Array("", exampleUrl, exampleUrl).deep)
  }

  test("URLs") {
    val textSampleArc = RecordLoader
      .loadArchives(arcPath, sc)
      .map(x => x.getUrl)
      .take(3)
    val textSampleWarc = RecordLoader
      .loadArchives(warcPath, sc)
      .map(x => x.getUrl)
      .take(3)
    assert(
      textSampleArc.deep == Array(
        "filedesc://IAH-20080430204825-00000-blackbook.arc",
        "dns:www.archive.org",
        "http://www.archive.org/robots.txt"
      ).deep
    )
    assert(
      textSampleWarc.deep == Array(
        "dns:www.archive.org",
        "http://www.archive.org/robots.txt",
        "http://www.archive.org/"
      ).deep
    )
  }

  test("MIMEtype") {
    val textSampleArc = RecordLoader
      .loadArchives(arcPath, sc)
      .map(x => x.getMimeType)
      .take(3)
    val textSampleWarc = RecordLoader
      .loadArchives(warcPath, sc)
      .map(x => x.getMimeType)
      .take(3)
    assert(
      textSampleArc.deep == Array(
        exampleMimeType,
        "text/dns",
        exampleMimeType
      ).deep
    )
    assert(
      textSampleWarc.deep == Array("unknown", exampleMimeType, "text/html").deep
    )
  }

  test("Get HTTP status") {
    val textSampleArc = RecordLoader
      .loadArchives(arcPath, sc)
      .map(x => x.getHttpStatus)
      .take(3)
    val textSampleWarc = RecordLoader
      .loadArchives(warcPath, sc)
      .map(x => x.getHttpStatus)
      .take(3)
    assert(
      textSampleArc.deep == Array(
        exampleStatusCode1,
        exampleStatusCode1,
        exampleStatusCode2
      ).deep
    )
    assert(
      textSampleWarc.deep == Array(
        exampleStatusCode1,
        exampleStatusCode2,
        exampleStatusCode2
      ).deep
    )
  }

  test("Get Payload Digest") {
    val textSampleArc = RecordLoader
      .loadArchives(arcPath, sc)
      .map(x => x.getPayloadDigest)
      .take(3)
    val textSampleWarc = RecordLoader
      .loadArchives(warcPath, sc)
      .map(x => x.getPayloadDigest)
      .take(3)
    assert(
      textSampleArc.deep == Array(
        "sha1:252efd6dd414d91812dd9b0f897cdb2b44f64601",
        "sha1:8d115d0e83c5dcd66b13619e04d60a36cb2c1ee4",
        "sha1:ede22581685942721c7b9743dced317633d00e33"
      ).deep
    )
    assert(
      textSampleWarc.deep == Array(
        null,
        "sha1:SUCGMUVXDKVB5CS2NL4R4JABNX7K466U",
        "sha1:2WAXX5NUWNNCS2BDKCO5OVDQBJVNKIVV"
      ).deep
    )
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
