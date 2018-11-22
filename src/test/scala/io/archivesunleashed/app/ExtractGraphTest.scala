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
// scalastyle:on underscore.import
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.util.Try

 // See: https://github.com/archivesunleashed/aut/pull/204/files#diff-4541b9834513985c360b64093fd45073
 // @RunWith(classOf[JUnitRunner])
 @deprecated("Replaced with ExtractGraphX", "0.16.1")
 class ExtractGraphTest extends FunSuite with BeforeAndAfter {
     private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
     private var sc: SparkContext = _
     private val master = "local[4]"
     private val appName = "example-spark"
     private val testVertexFile = "temporaryTestVertexDir"
     private val testEdgesFile = "temporaryTestEdgesDir"
     private val dateTest = "20080430"
     private val urlPrimaryTest = "deadlists.com"
     private val urlSecondaryTest = "epic.org"

     before {
       val conf = new SparkConf()
         .setMaster(master)
         .setAppName(appName)
         conf.set("spark.driver.allowMultipleContexts", "true");
         sc = new SparkContext(conf)
       }

     test("creates a network with pagerank scores") {
       val examplerdd = RecordLoader.loadArchives(arcPath, sc)
       val graph = ExtractGraph(examplerdd, dynamic=true)
       val testVertexArray = Array(ExtractGraph.VertexData(urlSecondaryTest,0.15144580026750323,3,0),
         ExtractGraph.VertexData("fepproject.org",0.15048193342250107,1,0),
         ExtractGraph.VertexData("jou.ufl.edu",0.15048193342250107,1,0))
       val testEdgeArray = Array(ExtractGraph.EdgeData(dateTest,urlPrimaryTest,urlPrimaryTest),
       ExtractGraph.EdgeData(dateTest, urlPrimaryTest, urlPrimaryTest),
       ExtractGraph.EdgeData(dateTest, urlPrimaryTest,"psilo.com"))
       val testCount = 1000
       assert(graph.vertices.map( r => r._2).take(3).deep == testVertexArray.deep)
       assert(graph.edges.map( r => r.attr ).take(3).deep == testEdgeArray.deep)
       assert(ExtractGraph.VertexData(urlSecondaryTest, 0.0, 0,0).domain == urlSecondaryTest)
       assert(ExtractGraph.EdgeData(dateTest, urlPrimaryTest, urlPrimaryTest).date == dateTest)
     }

     test("creates a network without pagerank scores") {
       val examplerdd = RecordLoader.loadArchives(arcPath, sc)
       val graph = ExtractGraph(examplerdd)
       val testVertexArray = Array(ExtractGraph.VertexData(urlSecondaryTest,0.1514714083714221,3,0),
         ExtractGraph.VertexData("fepproject.org",0.1504904694571407,1,0),
         ExtractGraph.VertexData("jou.ufl.edu",0.1504904694571407,1,0))
       val testEdgeArray = Array(ExtractGraph.EdgeData(dateTest, urlPrimaryTest, urlPrimaryTest),
         ExtractGraph.EdgeData(dateTest, urlPrimaryTest, urlPrimaryTest),
         ExtractGraph.EdgeData(dateTest, urlPrimaryTest, "psilo.com"))
       assert(graph.vertices.map( r => r._2).take(3).deep == testVertexArray.deep)
       assert(graph.edges.map( r => r.attr ).take(3).deep == testEdgeArray.deep)

     }

     test("writes a json file") {
       val examplerdd = RecordLoader.loadArchives(arcPath, sc)
       val graph = ExtractGraph(examplerdd)
       graph.writeAsJson(testVertexFile, testEdgesFile)
       assert (Files.exists(Paths.get(testVertexFile)))
       assert (Files.exists(Paths.get(testEdgesFile)))
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
