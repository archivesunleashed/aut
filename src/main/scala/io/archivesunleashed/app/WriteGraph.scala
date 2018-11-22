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
// scalastyle:on underscore.import
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * UDF for exporting an RDD representing a collection of links to a GEXF file.
  */
object WriteGraph {

  val nodeStart = "<node id=\""
  val edgeStart = "<edge source=\""
  val targetChunk = "\" target=\""
  val directedChunk = "\" type=\"directed\">\n"
  val endAttribute = "\" />\n"
  val labelStart = "\" label=\""
  val xmlHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
  val edgeEnd = "</edge>\n"
  val gexfHeader = xmlHeader +
    "<gexf xmlns=\"http://www.gexf.net/1.3draft\"\n" +
    "  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
    "  xsi:schemaLocation=\"http://www.gexf.net/1.3draft\n" +
    "                       http://www.gexf.net/1.3draft/gexf.xsd\"\n" +
    "  version=\"1.3\">\n"
  val graphmlHeader = xmlHeader +
    "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" +
    "  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
    "  xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" +
    "  http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\"\n>"

  /** Writes graph nodes and edges to file.
   *
   * @param rdd RDD of elements in format ((datestring, source, target), count)
   * @param gexfPath output file
   * @return Unit().
   */
  def apply(rdd: RDD[((String, String, String), Int)], gexfPath: String): Boolean = {
    if (!gexfPath.isEmpty()) { asGexf (rdd, gexfPath) } else { false }
  }

  def apply(ds: Array[Row], gexfPath: String): Boolean = {
    if (!gexfPath.isEmpty())  { asGexf (ds, gexfPath) } else { false }
  }

  /** Produces a zipped RDD with ids and labels from an network-based RDD, rdd.
   * @param rdd a RDD of elements in format ((datestring, source, target), count)
   * @return an RDD containing (Label, UniqueId)
   */
  def nodesWithIds (rdd: RDD[((String, String, String), Int)]): RDD[(String, Long)] = {
    rdd.map(r => r._1._2.escapeInvalidXML())
      .union(rdd.map(r => r._1._3.escapeInvalidXML()))
      .distinct
      .zipWithUniqueId()
  }

  /** Produces the (label, id) combination from a nodeslist of an RDD that has the
   * same label as lookup.
   * @param rdd an RDD of elements in format ((datestring, source, target), count)
   * @param lookup the label to lookup.
   * @return an Option containing Some(Label, UniqueId) or None
   */
  def nodeLookup (rdd: RDD[(String, Long)], lookup: String): Option[(String, Long)] = {
    rdd.filter(r => r._1 contains lookup)
      .collect
      .lift(0)
  }

  /** Produces the id from Nodelookup
   * @param id an Option containing the lookup.
   * @return a long representing the id or -1 if no id exists from the lookup.
   */
  def nodeIdFromLabel (id: Option[(String, Long)]): Long = {
    id match {
      case Some(x) => x._2
      case None => -1
    }
  }

  /**  Produces uniqueIds for edge lists from a flatMapped rdd, using nodesWithIds
   * @param rdd an RDD of elements in format ((datestring, source, target), count)
   * @return an RDD in format (date, sourceid, destinationid, count)
   */
  def edgeNodes (rdd: RDD[((String, String, String), Int)]) : RDD[(String, Long, Long, Int)] = {
    val nodes = nodesWithIds(rdd)
    rdd.map(r => (r._1._2.escapeInvalidXML(), (r._1._3.escapeInvalidXML(), r._1._1, r._2)))
       .rightOuterJoin(nodes) // create ids for source urls from nodes.
       .filter(r => r._2._1 != None)
       .map( { case (source, (Some((destination, date, weight)), sid))
         => (destination, (sid, source, date, weight)) })
      .leftOuterJoin(nodes) // create ids for destination urls from nodes.
      .filter( r => r._2._1 != None)
      .map ({ case (dest, ((sid, source, date, weight), Some(did)))
        => (date, sid, did, weight)})
  }

  /** Produces the GEXF output from a RDD of tuples and outputs it to gexfPath.
   *
   * @param rdd a RDD of elements in format ((datestring, source, target), count)
   * @param gexfPath output file
   * @return true on success.
   */
  def asGexf (rdd: RDD[((String, String, String), Int)], gexfPath: String): Boolean = {
    if (gexfPath.isEmpty()) {
      false
    } else {
      val nodes = nodesWithIds(rdd)
      val outFile = Files.newBufferedWriter(Paths.get(gexfPath), StandardCharsets.UTF_8)
      val edges = edgeNodes(rdd).map({ case (date, sid, did, weight) =>
        edgeStart  +
        sid + targetChunk +
        did + "\" weight=\"" + weight +
        directedChunk +
        "<attvalues>\n" +
        "<attvalue for=\"0\" value=\"" + date + endAttribute +
        "</attvalues>\n" +
        edgeEnd})
      outFile.write(gexfHeader +
        "<graph mode=\"static\" defaultedgetype=\"directed\">\n" +
        "<attributes class=\"edge\">\n" +
        "  <attribute id=\"0\" title=\"crawlDate\" type=\"string\" />\n" +
        "</attributes>\n" +
        "<nodes>\n")
      nodes.map(r => nodeStart +
        r._2 + labelStart +
        r._1 + endAttribute).collect
        .foreach(r => outFile.write(r))
      outFile.write("</nodes>\n<edges>\n")
      edges.collect.foreach(r => outFile.write(r))
      outFile.write("</edges>\n</graph>\n</gexf>")
      outFile.close()
      true
    }
  }

  /** Produces the GEXF output from an Array[Row] and outputs it to gexfPath.
    *
    * @param data a Dataset[Row] of elements in format (datestring, source, target, count)
    * @param gexfPath output file
    * @return true on success.
    */
  def asGexf (data: Array[Row], gexfPath: String): Boolean = {
    if (gexfPath.isEmpty()) {
      false
    } else {
      val outFile = Files.newBufferedWriter(Paths.get(gexfPath), StandardCharsets.UTF_8)
      val vertices = scala.collection.mutable.Set[String]()
      data.foreach { d =>
        vertices.add(d.get(1).asInstanceOf[String])
        vertices.add(d.get(2).asInstanceOf[String])
      }
      outFile.write(gexfHeader +
        "<graph mode=\"static\" defaultedgetype=\"directed\">\n" +
        "<attributes class=\"edge\">\n" +
        "  <attribute id=\"0\" title=\"crawlDate\" type=\"string\" />\n" +
        "</attributes>\n" +
        "<nodes>\n")
      vertices.foreach { v =>
        outFile.write(nodeStart +
          v.computeHash() + "\" label=\"" +
          v.escapeInvalidXML() + endAttribute)
      }
      outFile.write("</nodes>\n<edges>\n")
      data.foreach { e =>
        outFile.write(edgeStart + e.get(1).asInstanceOf[String].computeHash() + targetChunk +
          e.get(2).asInstanceOf[String].computeHash() + "\" weight=\"" + e.get(3) +
          "\" type=\"directed\">\n" +
          "<attvalues>\n" +
          "<attvalue for=\"0\" value=\"" + e.get(0) + endAttribute +
          "</attvalues>\n" +
          edgeEnd)
      }
      outFile.write("</edges>\n</graph>\n</gexf>")
      outFile.close()
      true
    }
  }

  def asGraphml (rdd: RDD[((String, String, String), Int)], graphmlPath: String): Boolean = {
    if (graphmlPath.isEmpty()) {
      false
    } else {
      val nodes = nodesWithIds(rdd)
      val outFile = Files.newBufferedWriter(Paths.get(graphmlPath), StandardCharsets.UTF_8)
      val edges = edgeNodes(rdd).map({ case (date, sid, did, weight) =>
        edgeStart +
        sid + targetChunk +
        did + directedChunk +
        "<data key=\"weight\">" + weight + "</data>\n" +
        "<data key=\"crawlDate\">" + date + "</data>\n" +
        edgeEnd})
      outFile.write(graphmlHeader +
        "<key id=\"label\" for=\"node\" attr.name=\"label\" attr.type=\"string\" />\n" +
        "<key id=\"weight\" for=\"edge\" attr.name=\"weight\" attr.type=\"double\">\n" +
        "<default>0.0</default>\n" +
        "</key>\n" +
        "<key id=\"crawlDate\" for=\"edge\" attr.name=\"crawlDate\" attr.type=\"string\" />\n" +
        "<graph mode=\"static\" edgedefault=\"directed\">\n")
      nodes.map(r => nodeStart + r._2 + "\">\n" +
        "<data key=\"label\">" + r._1 + "</data>\n</node>\n").collect
         .foreach(r => outFile.write(r))
      edges.collect.foreach(r => outFile.write(r))
      outFile.write("</graph>\n</graphml>")
      outFile.close()
      true
    }
  }
}
