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
  def apply(rdd: RDD[((String, String, String), Int)], graphmlPath: String): Boolean = {
    if (graphmlPath.isEmpty()) false
    else makeFile (rdd, graphmlPath)
  }

  def makeFile (rdd: RDD[((String, String, String), Int)], graphmlPath: String): Boolean = {
    val outFile = Files.newBufferedWriter(Paths.get(graphmlPath), StandardCharsets.UTF_8)
    val edges = rdd.map(r => "      <edge source=\"" + r._1._2 + "\" target=\"" +
      r._1._3 + """"  type="directed">
      <data key="e0">""" + r._2 + """</data>
      <data key="e1">"""" + r._1._1 + """"</data>
      </edge>""").collect
    val nodes = rdd.flatMap(r => List("      <node id=\"" +
      r._1._2 + "\">\n" +
      "        <data id=\"n0\">" + r._1._2 + "</data>\n" +
             "</node>",
      "      <node id=\"" + r._1._3 + "\">\n" +
      "        <data id=\"n0\">" + r._1._3 + """</data>
             </node>""")).distinct.collect
    outFile.write("""<?xml version="1.0" encoding="UTF-8"?>
      <graphml xmlns="http://graphml.graphdrawing.org/xmlns"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns
               http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
        <key id="n0" for="node" attr.name="label" attr.type="label">
          <default>""</default>
        </key>
        <key id="e0" for="edge" attr.name="weight" attr.type="double">
          <default>0.0</default>
        </key>
        <key id="e1" for="edge" attr.name="crawlDate" attr.type="string">
        </key>
        <graph mode="static" edgedefault="directed">
          """)
    nodes.foreach(r => outFile.write(r + "\n"))
    edges.foreach(r => outFile.write(r + "\n"))
    outFile.write("""</graph>
      </graphml>""")
    outFile.close()
    return true
  }
}
