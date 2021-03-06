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

import io.archivesunleashed.matchbox.{ComputeMD5, WWWLink}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.Row

object WriteGEXF {

  /** Verifies gexfPath is empty.
    *
    * @param data Array[Row] of elements in format (crawl_date, src_domain,
    *        dest_domain, count)
    * @param gexfPath output file
    */
  def apply(data: Array[Row], gexfPath: String): Boolean = {
    if (gexfPath.isEmpty()) {
      false
    } else {
      makeFile(data, gexfPath)
    }
  }

  /** Produces the GEXF output from an Array[Row] and outputs it to gexfPath.
    *
    * @param data a Array[Row] of elements in format (crawl_date, src_domain,
    *        dest_domain, count)
    * @param gexfPath output file
    * @return true on success.
    */
  def makeFile(data: Array[Row], gexfPath: String): Boolean = {
    val outFile =
      Files.newBufferedWriter(Paths.get(gexfPath), StandardCharsets.UTF_8)
    val endAttribute = "\" />\n"
    val vertices = scala.collection.mutable.Set[String]()

    data foreach { d =>
      vertices.add(d.get(1).asInstanceOf[String])
      vertices.add(d.get(2).asInstanceOf[String])
    }
    outFile.write(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<gexf xmlns=\"http://www.gexf.net/1.3draft\"\n" +
        "  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
        "  xsi:schemaLocation=\"http://www.gexf.net/1.3draft\n" +
        "                       http://www.gexf.net/1.3draft/gexf.xsd\"\n" +
        "  version=\"1.3\">\n" +
        "<graph mode=\"static\" defaultedgetype=\"directed\">\n" +
        "<attributes class=\"edge\">\n" +
        "  <attribute id=\"0\" title=\"crawlDate\" type=\"string\" />\n" +
        "</attributes>\n" +
        "<nodes>\n"
    )
    vertices foreach { v =>
      outFile.write(
        "<node id=\"" +
          ComputeMD5(v.getBytes) + "\" label=\"" +
          v.escapeInvalidXML() + endAttribute
      )
    }
    outFile.write("</nodes>\n<edges>\n")
    data foreach { e =>
      outFile.write(
        "<edge source=\"" + ComputeMD5(
          e.get(1).asInstanceOf[String].getBytes
        ) + "\" target=\"" +
          ComputeMD5(
            e.get(2).asInstanceOf[String].getBytes
          ) + "\" weight=\"" + e.get(3) +
          "\" type=\"directed\">\n" +
          "<attvalues>\n" +
          "<attvalue for=\"0\" value=\"" + e.get(0) + endAttribute +
          "</attvalues>\n" +
          "</edge>\n"
      )
    }
    outFile.write("</edges>\n</graph>\n</gexf>")
    outFile.close()
    true
  }
}
