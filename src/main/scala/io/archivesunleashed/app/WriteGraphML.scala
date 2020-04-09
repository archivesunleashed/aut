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
package io.archivesunleashed.app
import io.archivesunleashed.matchbox.{ComputeMD5RDD, WWWLink}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * UDF for exporting an RDD or DataFrame representing a collection of links to a GraphML file.
  */
object WriteGraphML {

  /** Writes graph nodes and edges to file.
   *
   * @param rdd RDD of elements in format ((datestring, source, target), count)
   * @param graphmlPath output file
   */
  def apply(rdd: RDD[((String, String, String), Int)], graphmlPath: String): Boolean = {
    if (graphmlPath.isEmpty()) {
      false
    } else {
      makeFile (rdd, graphmlPath)
    }
  }

  def apply(ds: Array[Row], graphmlPath: String): Boolean = {
    if (graphmlPath.isEmpty()) {
      false
    } else {
      makeFile (ds, graphmlPath)
    }
  }

  /** Produces the GraphML output from an RDD of tuples and outputs it to graphmlPath.
   *
   * @param rdd RDD of elements in format ((datestring, source, target), count)
   * @param graphmlPath output file
   * @return true on successful run.
   */
  def makeFile (rdd: RDD[((String, String, String), Int)], graphmlPath: String): Boolean = {
    val outFile = Files.newBufferedWriter(Paths.get(graphmlPath), StandardCharsets.UTF_8)
    val edges = rdd.map(r => "<edge source=\"" + ComputeMD5RDD(r._1._2.getBytes) + "\" target=\"" +
      ComputeMD5RDD(r._1._3.getBytes) + "\" type=\"directed\">\n" +
    "<data key=\"weight\">" + r._2 + "</data>\n" +
    "<data key=\"crawlDate\">" + r._1._1 + "</data>\n" +
    "</edge>\n").collect
    val nodes = rdd.flatMap(r => List("<node id=\"" + ComputeMD5RDD(r._1._2.getBytes) + "\">\n" +
      "<data key=\"label\">" + r._1._2.escapeInvalidXML() + "</data>\n</node>\n",
      "<node id=\"" + ComputeMD5RDD(r._1._3.getBytes) + "\">\n" +
      "<data key=\"label\">" + r._1._3.escapeInvalidXML() + "</data>\n</node>\n")).distinct.collect
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
    true
  }

  /** Produces the GraphML output from an Array[Row] and outputs it to graphmlPath.
   *
   * @param data a Dataset[Row] of elements in format (datestring, source, target, count)
   * @param graphmlPath output file
   * @return true on success.
   */
  def makeFile(data: Array[Row], graphmlPath: String): Boolean = {
    val outFile = Files.newBufferedWriter(Paths.get(graphmlPath), StandardCharsets.UTF_8)
    val nodes = scala.collection.mutable.Set[String]()

    data foreach { d =>
      nodes.add(d.get(1).asInstanceOf[String])
      nodes.add(d.get(2).asInstanceOf[String])
    }

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

    nodes foreach { n =>
      outFile.write("<node id=\"" + ComputeMD5RDD(n.asInstanceOf[String].getBytes) + "\">\n" +
        "<data key=\"label\">" + n.asInstanceOf[String].escapeInvalidXML() + "</data>\n</node>\n")
    }

    data foreach { e =>
      outFile.write("<edge source=\"" + ComputeMD5RDD(e.get(1).asInstanceOf[String].getBytes) + "\" target=\"" +
        ComputeMD5RDD(e.get(2).asInstanceOf[String].getBytes) + "\" type=\"directed\">\n" +
        "<data key=\"weight\">" + e.get(3) + "</data>\n" +
        "<data key=\"crawlDate\">" + e.get(0) + "</data>\n" +
        "</edge>\n")
    }
    outFile.write("</graph>\n" +
    "</graphml>")
    outFile.close()
    true
  }
}
