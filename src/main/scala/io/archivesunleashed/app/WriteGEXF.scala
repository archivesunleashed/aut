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
object WriteGEXF {

  /** Writes graph nodes and edges to file.
   *
   * @param rdd RDD of elements in format ((datestring, source, target), count)
   * @param gexfPath output file
   * @return Unit().
   */
  def apply(rdd: RDD[((String, String, String), Int)], gexfPath: String): Boolean = {
    if (gexfPath.isEmpty()) {
      false
    } else
    {
      makeFile (rdd, gexfPath)
    }
  }

  def apply(ds: Array[Row], gexfPath: String): Boolean = {
    if (gexfPath.isEmpty())  {
      false
    } else {
      makeFile (ds, gexfPath)
    }
  }

  /** Produces the GEXF output from a RDD of tuples and outputs it to gexfPath.
   *
   * @param rdd a RDD of elements in format ((datestring, source, target), count)
   * @param gexfPath output file
   * @return true on success.
   */
  def makeFile (rdd: RDD[((String, String, String), Int)], gexfPath: String): Boolean = {
    val outFile = Files.newBufferedWriter(Paths.get(gexfPath), StandardCharsets.UTF_8)
    val endAttribute = "\" />\n"
    val nodeStart = "<node id=\""
    val labelStart = "\" label=\""
    val edges = rdd.map(r => "<edge source=\"" + r._1._2.computeHash() + "\" target=\"" +
      r._1._3.computeHash() + "\" weight=\"" + r._2 +
      "\" type=\"directed\">\n" +
      "<attvalues>\n" +
      "<attvalue for=\"0\" value=\"" + r._1._1 + endAttribute +
      "</attvalues>\n" +
      "</edge>\n").collect
    val nodes = rdd.flatMap(r => List(nodeStart +
      r._1._2.computeHash() + labelStart +
      r._1._2.escapeInvalidXML() + endAttribute,
      nodeStart +
      r._1._3.computeHash() + labelStart +
      r._1._3.escapeInvalidXML() + endAttribute)).distinct.collect
    outFile.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.3draft\"\n" +
      "  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
      "  xsi:schemaLocation=\"http://www.gexf.net/1.3draft\n" +
      "                       http://www.gexf.net/1.3draft/gexf.xsd\"\n" +
      "  version=\"1.3\">\n" +
      "<graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "<attributes class=\"edge\">\n" +
      "  <attribute id=\"0\" title=\"crawlDate\" type=\"string\" />\n" +
      "</attributes>\n" +
      "<nodes>\n")
    nodes.foreach(r => outFile.write(r))
    outFile.write("</nodes>\n<edges>\n")
    edges.foreach(r => outFile.write(r))
    outFile.write("</edges>\n</graph>\n</gexf>")
    outFile.close()
    true
  }

  /** Produces the GEXF output from an Array[Row] and outputs it to gexfPath.
    *
    * @param data a Dataset[Row] of elements in format (datestring, source, target, count)
    * @param gexfPath output file
    * @return true on success.
    */
  def makeFile(data: Array[Row], gexfPath: String): Boolean = {
    val outFile = Files.newBufferedWriter(Paths.get(gexfPath), StandardCharsets.UTF_8)
    val endAttribute = "\" />\n"
    val vertices = scala.collection.mutable.Set[String]()

    data foreach { d =>
      vertices.add(d.get(1).asInstanceOf[String])
      vertices.add(d.get(2).asInstanceOf[String])
    }
    outFile.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.3draft\"\n" +
      "  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
      "  xsi:schemaLocation=\"http://www.gexf.net/1.3draft\n" +
      "                       http://www.gexf.net/1.3draft/gexf.xsd\"\n" +
      "  version=\"1.3\">\n" +
      "<graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "<attributes class=\"edge\">\n" +
      "  <attribute id=\"0\" title=\"crawlDate\" type=\"string\" />\n" +
      "</attributes>\n" +
      "<nodes>\n")
    vertices foreach { v =>
      outFile.write("<node id=\"" +
        v.computeHash() + "\" label=\"" +
        v.escapeInvalidXML() + endAttribute)
    }
    outFile.write("</nodes>\n<edges>\n")
    data foreach { e =>
      outFile.write("<edge source=\"" + e.get(1).asInstanceOf[String].computeHash() + "\" target=\"" +
        e.get(2).asInstanceOf[String].computeHash() + "\" weight=\"" + e.get(3) +
        "\" type=\"directed\">\n" +
        "<attvalues>\n" +
        "<attvalue for=\"0\" value=\"" + e.get(0) + endAttribute +
        "</attvalues>\n" +
        "</edge>\n")
    }
    outFile.write("</edges>\n</graph>\n</gexf>")
    outFile.close()
    true
  }
}
