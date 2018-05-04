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
package io.archivesunleashed.app

import io.archivesunleashed._
import io.archivesunleashed.matchbox._
import io.archivesunleashed.util.JsonUtils
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/** Extracts a site link structure using Spark's GraphX utility. */
object ExtractGraphXSLS {

  /** Creates a hashcode from a url to use as a unique id.
   *
   * @param url
   * @return unique id as long integer.
   */
  def pageHash(url: String): VertexId = {
    url.hashCode.toLong
  }

  case class VertexData(url: String)
  case class EdgeData(edgeCount: Int)

 
  def apply(records: RDD[ArchiveRecord]): Graph[VertexData, EdgeData] = {
    val extractedLinks = records.keepValidPages().flatMap(r => ExtractLinks(r.getUrl,r.getContentString)).map(r =>(ExtractDomain(r._1).removePrefixWWW(),ExtractDomain(r._2).removePrefixWWW())).filter(r => r._1 != "" && r._2 != "").distinct.persist()

    val vertices: RDD[(VertexId, VertexData)] = extractedLinks
      .flatMap(r => List(r._1, r._2))
      .distinct
      .map(r => (pageHash(r), VertexData(r)))

    val edges: RDD[Edge[EdgeData]] = extractedLinks
      .map(r => Edge(pageHash(r._1), pageHash(r._2), EdgeData(1)))

    val graph = Graph(vertices, edges)

  }

  /** Writes a Graph object to a Json file.
    *
    * @constructor graph - a SparkX graph object containing vertex and edge data
    * @return Unit().
    */
  implicit class GraphWriter(graph: Graph[VertexData, EdgeData]) {
    /** Writes a graph object to json files containing vertex and edge data.
      *
      * @param verticesPath Filepath for vertices output
      * @param edgesPath Filepath for edges output
      * @return Unit().
      */
    def writeAsJson(verticesPath: String, edgesPath: String) = {
      // Combine edges of a given (date, src, dst) combination into single record with count value.
      val edgesCounted = graph.edges.countItems().map {
        r => Map("date" -> r._1.attr.date,
          "src" -> r._1.attr.src,
          "dst" -> r._1.attr.dst,
          "count" -> r._2)
      }
      edgesCounted.map(r => JsonUtils.toJson(r)).saveAsTextFile(edgesPath)
      graph.vertices.map(r => JsonUtils.toJson(r._2)).saveAsTextFile(verticesPath)
    }
  }
}
