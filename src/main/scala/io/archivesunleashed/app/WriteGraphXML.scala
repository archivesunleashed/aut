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
import io.archivesunleashed.matchbox._
import org.apache.spark.graphx._
// scalastyle:on underscore.import
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import io.archivesunleashed.app.ExtractGraphX.{VertexData,EdgeData,VertexDataPR}

import org.apache.spark.rdd.RDD

/**
  * UDF for exporting an GraphX object representing a collection of links to a GraphML file.
  */
object WriteGraphXML {
  /** Writes graphX object nodes and edges to file.
   *
   * @param graph GraphX object of type Graph[VertexDataPR, EdgeData]
   * @param graphmlPath output file
   */
  def apply(graph: Graph[VertexDataPR, EdgeData], graphmlPath: String): Boolean = {
    if (graphmlPath.isEmpty()) {
      false
    } else {
      makeFile (graph, graphmlPath)
    }
  }

  /** Produces the GraphML output from a GraphX object and outputs it to graphmlPath.
   *
   * @param graph GraphX object of type Graph[VertexDataPR, EdgeData]
   * @param graphmlPath output file
   * @return true on successful run.
   */
  def makeFile (graph: Graph[VertexDataPR, EdgeData], graphmlPath: String): Boolean = {
    val outFile = Files.newBufferedWriter(Paths.get(graphmlPath), StandardCharsets.UTF_8)

    val END_DATA_XML: String = "</data>\n"

    val mappedGraph = graph.triplets.map(triplet => List("<edge source=\"" + triplet.srcId + "\" target=\"" +
      triplet.dstId + "\"  type=\"directed\">\n" +
    "<data key=\"weight\">" + triplet.attr.edgeCount + END_DATA_XML +
    "</edge>\n", "<node id=\"" + triplet.srcId + "\">\n" +
      "<data key=\"pageRank\">" + triplet.srcAttr.pageRank + END_DATA_XML +
      "<data key=\"connectedComponent\">" + triplet.srcAttr.weak + END_DATA_XML +
      "<data key=\"stronglyConnectedComponent\">" + triplet.srcAttr.strong + END_DATA_XML +
      "<data key=\"label\">" + triplet.srcAttr.url + END_DATA_XML + "</node>\n",
      "<node id=\"" + triplet.dstId + "\">\n" +
      "<data key=\"pageRank\">" + triplet.dstAttr.pageRank + END_DATA_XML +
      "<data key=\"connectedComponent\">" + triplet.dstAttr.weak + END_DATA_XML +
      "<data key=\"stronglyConnectedComponent\">" + triplet.dstAttr.strong + END_DATA_XML +
      "<data key=\"label\">" + triplet.dstAttr.url + END_DATA_XML + "</node>\n")).distinct.collect

    outFile.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" +
      "  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
      "  xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" +
      "  http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" +
      "<key id=\"label\" for=\"node\" attr.name=\"label\" attr.type=\"string\" />\n" +
      "<key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"double\">\n" +
      "<default>0.0</default>\n" +
      "</key>\n" +
      "<key id=\"pageRank\" for=\"node\" attr.name=\"pageRank\" " +
      "attr.type=\"double\" />\n" +
      "<key id=\"stronglyConnectedComponent\" for=\"node\" " +
      "attr.name=\"stronglyConnectedComponent\" attr.type=\"int\" />\n" +
      "<key id=\"connectedComponent\" for=\"node\" " +
      "attr.name=\"connectedComponent\" attr.type=\"int\" />\n" +
      "<graph mode=\"static\" edgedefault=\"directed\">\n")
    outFile.write("<nodes>\n")
    mappedGraph.foreach(r => outFile.write(r(1) + r(2)))
    outFile.write("\n</nodes>\n<edges>\n")
    mappedGraph.foreach(r => outFile.write(r(0)))
    outFile.write("\n</edges>\n")
    outFile.write("</graph>\n" +
    "</graphml>")
    outFile.close()
    true
  }
}
