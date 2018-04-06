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

/** Extracts a network graph using Spark's GraphX utility. */
object ExtractGraph {

  /** Creates a hashcode from a url to use as a unique id.
   *
   * @param url
   * @return unique id as long integer.
   */
  def pageHash(url: String): VertexId = {
    url.hashCode.toLong
  }

  case class VertexData(domain: String, pageRank: Double, inDegree: Int, outDegree: Int)
  case class EdgeData(date: String, src: String, dst: String)

  /** Creates a network graph from loaded Archive Records with optional pageRank calculations.
   *
   * @param records an RDD of archive records
   * @param dynamic whether to calculate PageRank (an O(n^2) calculation, so not
   *                recommended for very large graphs)
   * @param tolerance the percentage of the time the PR algorithm "jumps" to
   *                  a random location in its random walks
   * @param numIter the number of iterations applied to the PR algorithm
   * @return a Graph object containing data for vertices and edges as extracted.
   */
  def apply(records: RDD[ArchiveRecord], dynamic: Boolean = false,
            tolerance: Double = 0.005, numIter: Int = 20): Graph[VertexData, EdgeData] = {
    val extractedLinks = records.keepValidPages()
      .map(r => (r.getCrawlDate, ExtractLinks(r.getUrl, r.getContentString)))
      .flatMap(r => r._2.map(f => (r._1, ExtractDomain(f._1).removePrefixWWW(), ExtractDomain(f._2).removePrefixWWW())))
      .filter(r => r._2 != "" && r._3 != "")
      .persist()

    val vertices: RDD[(VertexId, VertexData)] = extractedLinks
      .flatMap(r => List(r._2, r._3))
      .distinct
      .map(r => (pageHash(r), VertexData(r, 0.0, 0, 0)))

    val edges: RDD[Edge[EdgeData]] = extractedLinks
      .map(r => Edge(pageHash(r._2), pageHash(r._3), EdgeData(r._1, r._2, r._3)))

    val graph = Graph(vertices, edges)

    val graphInOut = graph.outerJoinVertices(graph.inDegrees) {
      case (vid, rv, inDegOpt) => VertexData(rv.domain, rv.pageRank, inDegOpt.getOrElse(0), rv.outDegree)
    }.outerJoinVertices(graph.outDegrees) {
      case (vid, rv, outDegOpt) => VertexData(rv.domain, rv.pageRank, rv.inDegree, outDegOpt.getOrElse(0))
    }

    if (dynamic) {
      graphInOut.outerJoinVertices(graph.pageRank(tolerance).vertices) {
        case (vid, rv, pageRankOpt) => VertexData(rv.domain, pageRankOpt.getOrElse(0.0), rv.inDegree, rv.outDegree)
      }
    } else {
      graphInOut.outerJoinVertices(graph.staticPageRank(numIter).vertices) {
        case (vid, rv, pageRankOpt) => VertexData(rv.domain, pageRankOpt.getOrElse(0.0), rv.inDegree, rv.outDegree)
      }
    }
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
