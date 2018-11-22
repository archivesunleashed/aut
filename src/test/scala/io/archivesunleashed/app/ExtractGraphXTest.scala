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

package io.archivesunleashed.app

import java.io.File
import java.nio.file.{Files, Paths}

import com.google.common.io.Resources
// scalastyle:off underscore.import
import io.archivesunleashed._
import io.archivesunleashed.matchbox._
import io.archivesunleashed.app._
import io.archivesunleashed.util._
import org.apache.spark.graphx._
// scalastyle:on underscore.import
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.util.Try

 @RunWith(classOf[JUnitRunner])
 class ExtractGraphXTest extends FunSuite with BeforeAndAfter {
     private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
     private var sc: SparkContext = _
     private val master = "local[4]"
     private val appName = "example-spark"
     private val testVertexFile = "temporaryTestVertexDir"
     private val testEdgesFile = "temporaryTestEdgesDir"

     before {
       val conf = new SparkConf()
         .setMaster(master)
         .setAppName(appName)
         conf.set("spark.driver.allowMultipleContexts", "true");
         sc = new SparkContext(conf)
       }

    test ("Case classes are empty") {
      val pageRank = 0.56
      val weak = 4
      val strong = 5
      val edgeCount = 100
      val label = "url"
      val testPR = ExtractGraphX.VertexDataPR("url", pageRank, weak, strong)
      val testVertexData = ExtractGraphX.VertexData(label)
      val testEdgeData = ExtractGraphX.EdgeData(edgeCount)
      assert(testVertexData.url == label)
      assert(testEdgeData.edgeCount == edgeCount)
      assert(testPR.url == label)
      assert(testPR.pageRank == pageRank)
      assert(testPR.weak == weak)
      assert(testPR.strong == strong)
    }

     test("creates a network with pagerank scores") {
       val pageRank = 0.9943090942904987
       val connected = -649648005
       val minEdges = 5
       val minTake = 3
       val examplerdd = RecordLoader.loadArchives(arcPath, sc)
         .keepValidPages()
         .flatMap(r => ExtractLinks(r.getUrl, r.getContentString))
         .map(r => (ExtractDomain(r._1).removePrefixWWW(), ExtractDomain(r._2).removePrefixWWW()))
         .filter(r => r._1 != "" && r._2 != "")
       val graph = ExtractGraphX.extractGraphX(examplerdd)
         .subgraph(epred = eTriplet => eTriplet.attr.edgeCount > minEdges)
       val pRank = ExtractGraphX.runPageRankAlgorithm(graph).vertices.take(minTake)
       assert(pRank(0)._2.pageRank == pageRank)
       assert(pRank(0)._2.weak == connected)
       assert(pRank(0)._2.strong == connected)
     }

     test("creates a network using dynamic PR") {
       val dynPageRank = 0.9999999999999986
       val connected = -1054421350
       val minEdges = 5
       val minTake = 3
       val examplerdd = RecordLoader.loadArchives(arcPath, sc)
         .keepValidPages()
         .keepContent(Set("apple".r))
         .flatMap(r => ExtractLinks(r.getUrl, r.getContentString))
         .map(r => (ExtractDomain(r._1).removePrefixWWW(), ExtractDomain(r._2).removePrefixWWW()))
         .filter(r => r._1 != "" && r._2 != "")
       ExtractGraphX.dynamic = true
       val graph = ExtractGraphX.extractGraphX(examplerdd)
         .subgraph(epred = eTriplet => eTriplet.attr.edgeCount > minEdges)
       val pRank = ExtractGraphX.runPageRankAlgorithm(graph).vertices.take(minTake)

       assert(pRank(0)._2.pageRank == dynPageRank)
       assert(pRank(0)._2.weak == connected)
       assert(pRank(0)._2.strong == connected)
     }

     after {
       if (sc != null) {
         sc.stop()
       }
       if (Files.exists(Paths.get(testVertexFile))) {
         Try (FileUtils.deleteDirectory(new File(testVertexFile)))
       }
       if (Files.exists(Paths.get(testEdgesFile))) {
         Try(FileUtils.deleteDirectory(new File(testEdgesFile)));
       }
     }
 }
