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
package io.archivesunleashed.df

import com.google.common.io.Resources
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class DataFrameLoaderTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val warcPath = Resources.getResource("warc/example.warc.gz").getPath
  private val mediaPath =
    Resources.getResource("warc/example.media.warc.gz").getPath
  private val docPath =
    Resources.getResource("warc/example.docs.warc.gz").getPath
  private val txtPath =
    Resources.getResource("warc/example.txt.warc.gz").getPath
  private val pdfPath =
    Resources.getResource("warc/example.pdf.warc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-df"
  private var sc: SparkContext = _
  private val domain = "domain"
  private val url = "url"
  private val mime_type = "mime_type_web_server"
  private val md5 = "md5"
  private val crawl_date = "crawl_date"

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  test("Test DataFrameLoader (connection to PySpark)") {
    val df = new DataFrameLoader(sc)
    val validPages = df.webpages(arcPath)
    val hyperlinks = df.webgraph(arcPath)
    val imagegraph = df.imagegraph(arcPath)
    val images = df.images(arcPath)
    val pdfs = df.pdfs(pdfPath)
    val audio = df.audio(mediaPath)
    val video = df.videos(mediaPath)
    val spreadsheets = df.spreadsheets(docPath)
    val powerpoint = df.presentationProgramFiles(docPath)
    val word = df.wordProcessorFiles(docPath)
    val all = df.all(arcPath)
    val css = df.css(warcPath)
    val html = df.html(warcPath)
    val js = df.js(warcPath)
    val json = df.json(pdfPath)
    val pt = df.plainText(warcPath)
    val xml = df.xml(warcPath)

    val r_1 = validPages.select(domain, url, mime_type).take(1)(0)
    assert(r_1.getAs[String](domain) == "archive.org")
    assert(r_1.getAs[String](url) == "http://www.archive.org/")
    assert(r_1.getAs[String](mime_type) == "text/html")

    val r_2 = hyperlinks.select("Dest", "Anchor").take(3)(2)
    assert(r_2(0) == "http://www.archive.org/web/web.php")
    assert(r_2(1) == "Web")

    val r_3 = imagegraph.take(100)(99)
    assert(r_3.get(0) == "20080430204841")
    assert(
      r_3.get(1) == "http://www.archive.org/details/secretarmiesb00spivrich"
    )
    assert(r_3.get(2) == "http://www.archive.org/images/star.png")

    val r_4 = images.take(1)(0)
    assert(r_4.getAs[String](url) == "http://www.archive.org/images/logoc.jpg")
    assert(r_4.getAs[String](md5) == "8211d1fbb9b03d8522a1ae378f9d1b24")

    val r_5 = pdfs.take(1)(0)
    assert(
      r_5.getAs[String](
        url
      ) == "https://yorkspace.library.yorku.ca/xmlui/bitstream/handle/10315/36158/cost-analysis.pdf?sequence=1&isAllowed=y"
    )
    assert(r_5.getAs[String](md5) == "aaba59d2287afd40c996488a39bbc0dd")

    val r_6 = audio.take(1)(0)
    assert(r_6.getAs[String](url) == "https://ruebot.net/files/feniz.mp3")
    assert(r_6.getAs[String](md5) == "f7e7ec84b12c294e19af1ba41732c733")

    val r_7 = video.take(1)(0)
    assert(
      r_7.getAs[String](url) == "https://ruebot.net/2018-11-12%2016.14.11.mp4"
    )
    assert(r_7.getAs[String](md5) == "2cde7de3213a87269957033f6315fce2")

    val r_8 = spreadsheets.take(1)(0)
    assert(
      r_8.getAs[String](
        url
      ) == "https://ruebot.net/files/aut-test-fixtures/test-aut-fixture.ods"
    )
    assert(r_8.getAs[String](md5) == "7f70280757d8beb2d1bfd6fb1b6ae6e9")

    val r_9 = powerpoint.take(1)(0)
    assert(
      r_9.getAs[String](
        url
      ) == "https://ruebot.net/files/aut-test-fixtures/aut-test-fixtures.pptx"
    )
    assert(r_9.getAs[String](md5) == "7a7b1fe4b6d311376eaced9de3b682ee")

    val r_10 = word.take(1)(0)
    assert(
      r_10.getAs[String](
        url
      ) == "https://ruebot.net/files/aut-test-fixtures/test-aut-fixtures.rtf"
    )
    assert(r_10.getAs[String](md5) == "e483512b65ba44d71e843c57de2adeb7")

    val r_11 = all.select(url, mime_type).take(1)(0)
    assert(
      r_11.getAs[String](url) == "http://www.archive.org/robots.txt"
    )
    assert(r_11.getAs[String](mime_type) == "text/plain")

    val r_12 = css.select(crawl_date, url).take(1)(0)
    assert(r_12.getAs[String](crawl_date) == "20080430204833")
    assert(
      r_12
        .getAs[String](url) == "http://www.archive.org/stylesheets/details.css"
    )

    val r_13 = html.select(crawl_date, url).take(1)(0)
    assert(r_13.getAs[String](crawl_date) == "20080430204826")
    assert(
      r_13.getAs[String](url) == "http://www.archive.org/"
    )

    val r_14 = js.select(crawl_date, url).take(1)(0)
    assert(r_14.getAs[String](crawl_date) == "20080430204833")
    assert(
      r_14.getAs[String](url) == "http://www.archive.org/flv/flv.js?v=1.34"
    )

    val r_15 = json.select(crawl_date, url).take(1)(0)
    assert(r_15.getAs[String](crawl_date) == "20190812222538")
    assert(
      r_15.getAs[String](url) == "https://api.plu.mx/widget/other/artifact?type=doi&id=10.1109%2FJCDL.2019.00043&href=https%3A%2F%2Fplu.mx%2Fpitt%2Fa%2F%3Fdoi%3D10.1109%2FJCDL.2019.00043&ref=https%3A%2F%2Fyorkspace.library.yorku.ca%2Fxmlui%2Fhandle%2F10315%2F36158&pageToken=f74d46f3-f622-c670-e1bc-bdc3-aa500a283693&isElsWidget=false"
    )

    val r_16 = pt.select(crawl_date, url).take(1)(0)
    assert(r_16.getAs[String](crawl_date) == "20080430204825")
    assert(
      r_16.getAs[String](url) == "http://www.archive.org/robots.txt"
    )

    val r_17 = xml.select(crawl_date, url).take(1)(0)
    assert(r_17.getAs[String](crawl_date) == "20080430204830")
    assert(
      r_17.getAs[String](url) == "http://www.archive.org/services/collection-rss.php"
    )

  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
