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

import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.spark.rdd.RDD
import java.security.MessageDigest

/**
  * UDF for exporting an RDD representing a collection of links to a GDF file.
  */

object WriteGraphML {
  /**
  * @param rdd RDD of elements in format ((datestring, source, target), count).
  * @param graphmlPath Output file.
  *
  * Writes graph nodes and edges to file.
  */


// Compute a hash of a file
// The output of this function should match the output of running "md5 -q <file>"

  def apply(rdd: RDD[((String, String, String), Int)], graphmlPath: String): Boolean = {
    if (graphmlPath.isEmpty()) false
    else makeFile (rdd, graphmlPath)
  }

  def computeHash(id: String): String = {
    val md5 = MessageDigest.getInstance("MD5")
    return md5.digest(id.getBytes).map("%02x".format(_)).mkString
  }

  def makeFile (rdd: RDD[((String, String, String), Int)], graphmlPath: String): Boolean = {
    val outFile = Files.newBufferedWriter(Paths.get(graphmlPath), StandardCharsets.UTF_8)
    val edges = rdd.map(r => "<edge source=\"" + computeHash(r._1._2) + "\" target=\"" +
      computeHash(r._1._3) + "\"  type=\"directed\">\n" +
    "<data key=\"weight\">" + r._2 + "</data>\n" +
    "<data key=\"crawlDate\">" + r._1._1 + "</data>\n" +
    "</edge>\n").collect
    val nodes = rdd.flatMap(r => List("<node id=\"" + computeHash(r._1._2) + "\">\n" +
      "<data key=\"label\">" + r._1._2 + "</data>\n</node>\n",
      "<node id=\"" + computeHash(r._1._3) + "\">\n" +
      "<data key=\"label\">" + r._1._3 + "</data>\n</node>\n")).distinct.collect
    outFile.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" +
      "  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
      "  xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" +
      "  http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\"\n>" +
      "<key id=\"label\" for=\"node\" attr.name=\"label\" attr.type=\"string\" />\n" +
      "<key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"double\">\n" +
      "<default>0.0</default>\n" +
      "</key>\n" +
      "<key id=\"crawlDate\" for=\"edge\" attr.name=\"crawlDate\" attr.type=\"string\" />\n" +
      "<graph mode=\"static\" edgedefault=\"directed\">\n")
    nodes.foreach(r => outFile.write(r))
    edges.foreach(r => outFile.write(r))
    outFile.write("</graph>\n" +
    "</graphml>")
    outFile.close()
    return true
  }
}
