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
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.commons.io.FilenameUtils

@RunWith(classOf[JUnitRunner])
class ArchiveRecordTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val warcPath = Resources.getResource("warc/example.warc.gz").getPath
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

  test("count records") {
    assert(RecordLoader.loadArchives(arcPath, sc).count == 300L)
    assert(RecordLoader.loadArchives(warcPath, sc).count == 299L)
  }

  test("Resource name produces expected result.") {
    val textSampleArc = RecordLoader.loadArchives(arcPath, sc)
     .map(x => FilenameUtils.getName(x.getArchiveFilename))
     .take(3)
    val textSampleWarc = RecordLoader.loadArchives(warcPath, sc)
     .map(x => FilenameUtils.getName(x.getArchiveFilename)).take(3)
    assert(textSampleArc.deep == Array("example.arc.gz",
      "example.arc.gz", "example.arc.gz").deep)
    assert(textSampleWarc.deep == Array("example.warc.gz",
      "example.warc.gz", "example.warc.gz").deep)
  }

  test("Crawl Dates") {
    val textSampleArc = RecordLoader.loadArchives(arcPath, sc)
     .map(x => x.getCrawlDate).take(3)
    val textSampleWarc = RecordLoader.loadArchives(warcPath, sc)
     .map(x => x.getCrawlDate).take(3)
    assert(textSampleArc.deep == Array("20080430", "20080430", "20080430").deep)
    assert(textSampleWarc.deep == Array("20080430", "20080430", "20080430").deep)
  }

  test("Domains") {
    val textSampleArc = RecordLoader.loadArchives(arcPath, sc)
     .map(x => x.getDomain).take(3)
    val textSampleWarc = RecordLoader.loadArchives(warcPath, sc)
     .map(x => x.getDomain).take(3)
    assert(textSampleArc.deep == Array("", "", "www.archive.org").deep)
    assert(textSampleWarc.deep == Array("", "www.archive.org", "www.archive.org").deep)
  }

  test("Urls") {
    val textSampleArc = RecordLoader.loadArchives(arcPath, sc)
     .map(x => x.getUrl).take(3)
    val textSampleWarc = RecordLoader.loadArchives(warcPath, sc)
     .map(x => x.getUrl).take(3)
    assert(textSampleArc.deep == Array("filedesc://IAH-20080430204825-00000-blackbook.arc",
      "dns:www.archive.org", "http://www.archive.org/robots.txt").deep)
    assert(textSampleWarc.deep == Array("dns:www.archive.org",
      "http://www.archive.org/robots.txt", "http://www.archive.org/").deep)
  }

  test("Mime-Type") {
    val textSampleArc = RecordLoader.loadArchives(arcPath, sc)
      .map(x => x.getMimeType).take(3)
    val textSampleWarc = RecordLoader.loadArchives(warcPath, sc)
      .map(x => x.getMimeType).take(3)
    assert (textSampleArc.deep == Array ("text/plain", "text/dns", "text/plain").deep)
    assert (textSampleWarc.deep == Array("unknown", "text/plain", "text/html").deep)
  }

  test("Get Http Status") {
    val textSampleArc = RecordLoader.loadArchives(arcPath, sc)
      .map(x => x.getHttpStatus).take(3)
    val textSampleWarc = RecordLoader.loadArchives(warcPath, sc)
      .map(x => x.getHttpStatus).take(3)
    assert (textSampleArc.deep == Array("000", "000", "200").deep)
    assert (textSampleWarc.deep == Array("000", "200", "200").deep)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
