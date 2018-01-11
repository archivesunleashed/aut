/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source platform for analyzing web archives.
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
package io.archivesunleashed.spark.matchbox

import java.io.File
import java.nio.file.{Paths}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.io.{Files, Resources}
import org.apache.commons.io.FileUtils
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import io.archivesunleashed.spark.matchbox.NER3Classifier.NERClassType

import scala.collection.mutable
import sys.process._
import java.net.URL
import java.io._
import scala.io.Source



// There must be a valid classifier file with path `iNerClassifierFile` for this test to pass
@RunWith(classOf[JUnitRunner])
class ExtractEntitiesTest extends FunSuite with BeforeAndAfter {
  def fileDownloader(url: String, filename: String) = {
      new URL(url) #> new File(filename) !!
  }
  private val resourceDirectory = Paths.get("src","test","resources");
  private val LOG = LogFactory.getLog(classOf[ExtractEntitiesTest])
  private val NERUrl = "https://minhaskamal.github.io/DownGit/" +
    "#/home?url=https://github.com/archivesunleashed/" +
    "aut-resources/blob/master/NER/english.all.3class.distsim.crf.ser.gz"
  private val nerPath = resourceDirectory + "/ner/"
  private val scrapePath = Resources.getResource("ner").getPath
  private val archivePath = Resources.getResource("arc/example.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _
  private var tempDir: File = _
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  val NER: String = try {
     Resources.getResource("ner/english.all.3class.distsim.crf.ser.gz").getPath;
  } catch {
    case e: Exception => {
      null
    }
  } finally {
      fileDownloader(NERUrl, nerPath+"english.all.3class.distsim.crf.ser.gz");
      Resources.getResource("ner/english.all.3class.distsim.crf.ser.gz").getPath;

  }

  val iNerClassifierFile = NERUrl

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
    tempDir = Files.createTempDir()
    LOG.info("Output can be found in " + tempDir.getPath)
  }

  test("extract entities") {
    assume (
      ExtractEntities.extractFromScrapeText(iNerClassifierFile, scrapePath, tempDir + "/scrapeTextEntities", sc).take(3).last == "happy",
      "Could not find a NER Classifier, skipping test ...")

    val e = ExtractEntities.extractFromScrapeText(iNerClassifierFile, scrapePath, tempDir + "/scrapeTextEntities", sc).take(3).last
    val expectedEntityMap = mutable.Map[NERClassType.Value, List[String]]()
    expectedEntityMap.put(NERClassType.PERSON, List())
    expectedEntityMap.put(NERClassType.LOCATION, List("Teoma"))
    expectedEntityMap.put(NERClassType.ORGANIZATION, List())
    assert(e._1 == "20080430")
    assert(e._2 == "http://www.archive.org/robots.txt")
    val actual = mapper.readValue(e._3, classOf[Map[String, List[String]]])

    expectedEntityMap.toStream.foreach(f => {
      assert(f._2 == actual.get(f._1.toString).get)
    })
  }

  /*
  //ScrapeFile no longer exists in Resources
  test("Extract from Record") {
    val e = ExtractEntities.extractFromRecords(iNerClassifierFile, archivePath, tempDir + "/scrapeArcEntities", sc).take(3).last
    assert(e._1 == "hello")
  }
  */

  after {
    FileUtils.deleteDirectory(tempDir)
    LOG.info("Removing tmp files in " + tempDir.getPath)
    if (sc != null) {
      sc.stop()
    }
  }
}
