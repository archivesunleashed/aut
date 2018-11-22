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

// scalastyle:off underscore.import
import io.archivesunleashed._
import io.archivesunleashed.matchbox
import org.apache.spark.graphx._
// scalastyle:on underscore.import
import org.apache.spark.rdd.RDD

/** Extracts a site link structure using Spark's GraphX utility. */
object ExtractGraphX {

  /** Creates a hashcode from a url to use as a unique id.
   *
   * @param url
   * @return unique id as long integer.
   */
  def pageHash(url: String): VertexId = {
    url.hashCode.toLong
  }
  val DEF_TOLERANCE = 0.005
  val DEF_NUMITER = 20
  val DEF_RESET = 0.15
  /* tolerance level (probability of following a link) for page rank */
  var tolerance: Double = DEF_TOLERANCE
  /* number of iterations for page rank and strongly connected components */
  var numIter: Int = DEF_NUMITER
  /* probability page rank "walk" will start from a random position */
  var resetProbability: Double = DEF_RESET
  /* whether to calculate dynamic page rank */
  var dynamic: Boolean = false
  /* default page rank value */
  val defaultPR: Double = 0.0
  val defaultComponent: Long = 0
  case class VertexData(url: String)
  case class EdgeData(edgeCount: Int)
  case class VertexDataPR(url: String, pageRank: Double, weak: Long, strong: Long)

  /** Creates a GraphX object
   *
   * @param records an RDD of tuples (source, destination)
   * @return a GraphX object
   */
  def extractGraphX(records: RDD[(String, String)]): Graph[VertexData, EdgeData] = {
    val extractedLinks = records.persist()
    val vertices: RDD[(VertexId, VertexData)] = extractedLinks
      .flatMap(r => List(r._1, r._2))
      .distinct
      .map(r => (pageHash(r), VertexData(r)))
    val edges: RDD[Edge[EdgeData]] = extractedLinks
      .map(r => Edge(pageHash(r._1), pageHash(r._2), EdgeData(1)))
    val graph = Graph(vertices, edges)
      .partitionBy(PartitionStrategy.RandomVertexCut)
      .groupEdges((e1,e2) => EdgeData(e1.edgeCount + e2.edgeCount))
    graph
  }

  /** calculates basic graph data (Page Rank, weak and strong components) for Graph
   *
   * @param graph GraphX object
   * @return new graph object with additional attributes
   */
  def runPageRankAlgorithm(graph: Graph[VertexData, EdgeData]): Graph[VertexDataPR, EdgeData] = {
    if(dynamic){
      graph.outerJoinVertices(graph.pageRank(numIter, resetProbability).vertices) {
          case (id, vd, pr) => (vd, pr)}
        .outerJoinVertices(graph.connectedComponents().vertices) {
          case (id, (vd, pr), cc) => (vd, pr, cc)}
        .outerJoinVertices(graph.stronglyConnectedComponents(numIter).vertices) {
          case (id, (vd, pr, cc), scc) => VertexDataPR(vd.url, pr.getOrElse(defaultPR), cc.getOrElse(defaultComponent), scc.getOrElse(defaultComponent)) }
    }
    else{
      graph.outerJoinVertices(graph.staticPageRank(numIter, resetProbability).vertices){
        case (id, vd, pr) => (vd, pr)
      }.outerJoinVertices(graph.connectedComponents().vertices) {
        case (id, (vd, pr), cc) => (vd, pr, cc)}
      .outerJoinVertices(graph.stronglyConnectedComponents(numIter).vertices) {
        case (id, (vd, pr, cc), scc) => VertexDataPR(vd.url, pr.getOrElse(defaultPR), cc.getOrElse(defaultComponent), scc.getOrElse(defaultComponent)) }
    }
  }
}
