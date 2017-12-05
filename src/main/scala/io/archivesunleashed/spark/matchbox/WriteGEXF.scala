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

object WriteGEXF {
  /**
  * @param rdd RDD of elements in format ((datestring, source, target), count).
  * @param gexfPath Output file.
  *
  * Writes graph nodes and edges to file.
  */
  def apply(rdd: RDD[((String, String, String), Int)], gexfPath: String): Boolean = {
    if (gexfPath.isEmpty()) false
    else makeFile (rdd, gexfPath)
  }

  def makeFile (rdd: RDD[((String, String, String), Int)], gexfPath: String): Boolean = {
    val outFile = Files.newBufferedWriter(Paths.get(gexfPath), StandardCharsets.UTF_8)
    val edges = rdd.map(r => "      <edge source=\"" + r._1._2 + "\" target=\"" +
      r._1._3 + "\" label=\"\" weight=\"" + r._2 +
      """"  type="directed">
      <attvalues>
      <attvalue for="0" value="""" + r._1._1 + """" />
      </attvalues>
      </edge>""").collect
    val nodes = rdd.flatMap(r => List("      <node id=\"" +
      r._1._2 + "\" label=\"" +
      r._1._2 + "\" />\n",
      "      <node id=\"" +
      r._1._3 + "\" label=\"" +
      r._1._3 + "\" />")).distinct.collect
    outFile.write("""<?xml version="1.0" encoding="UTF-8"?>
      <gexf xmlns="http://www.gexf.net/1.2draft" version="1.2">
        <graph mode="static" defaultedgetype="directed">
          <attributes class="edge">
            <attribute id="0" title="crawlDate" type="string" />
          </attributes>
          <nodes>
          """)
    nodes.foreach(r => outFile.write(r + "\n"))
    outFile.write("""    </nodes>
      <edges>
      """)
    edges.foreach(r => outFile.write(r + "\n"))
    outFile.write("""    </edges>
        </graph>
      </gexf>""")
    outFile.close()
    return true
  }
}
