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
package io.archivesunleashed.matchbox

import io.archivesunleashed._
import io.archivesunleashed.matchbox._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/** Extracts a site link structure using Spark's GraphX utility. */
object ExtractGraphXSLS {

  /** Creates a hashcode from a url to use as a unique id.
   *
   * @param url
   * @return unique id as long integer.
   */
  def pageHash(url: String): VertexId = {
    url.hashCode.toLong
  }

  case class VertexData(url: String)
  case class EdgeData(edgeCount: Int) //: Graph[VertexData, EdgeData] 

 
  def apply(records: RDD[ArchiveRecord]) : Unit= {
    val extractedLinks = records.keepValidPages().flatMap(r => ExtractLinks(r.getUrl,r.getContentString)).map(r =>(ExtractDomain(r._1).removePrefixWWW(),ExtractDomain(r._2).removePrefixWWW())).filter(r => r._1 != "" && r._2 != "").persist()

    val vertices: RDD[(VertexId, VertexData)] = extractedLinks
      .flatMap(r => List(r._1, r._2))
      .distinct
      .map(r => (pageHash(r), VertexData(r)))

    val edges: RDD[Edge[EdgeData]] = extractedLinks
      .map(r => Edge(pageHash(r._1), pageHash(r._2), EdgeData(1)))
    

    val graph = Graph(vertices, edges).partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((e1,e2) => EdgeData(e1.edgeCount+e2.edgeCount))
    
    
    val mappedTuple = graph.triplets.map(t => (t.srcAttr.url, t.dstAttr.url, t.attr.edgeCount))
    
    mappedTuple.saveAsTextFile("links-all-graphX/")
    //return graph
  }
}
