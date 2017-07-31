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
package io.archivesunleashed.spark.scripts

import com.google.common.io.Resources
import org.apache.spark.{SparkConf, SparkContext}
import io.archivesunleashed.spark.matchbox.{ExtractLinks, ExtractDomain, RecordLoader}
import io.archivesunleashed.spark.matchbox._
import io.archivesunleashed.spark.matchbox.StringUtils._
import io.archivesunleashed.spark.rdd.RecordRDD._

object CrawlStatistics {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val warcPath = Resources.getResource("warc/example.warc.gz").getPath

  def numPagesPerCrawl(sc: SparkContext) = {
    val a = RecordLoader.loadArc(arcPath, sc)
    val b = RecordLoader.loadWarc(warcPath, sc)
    val pageCounts = a.union(b)
      .keepValidPages()
      .map(_.getCrawlDate)
      .countItems()
    println(pageCounts.take(1).mkString)
  }

  def numLinksPerCrawl(sc: SparkContext) = {
    val a = RecordLoader.loadArc(arcPath, sc)
    val b = RecordLoader.loadWarc(warcPath, sc)
    val linkCounts = a.union(b)
      .keepValidPages()
      .map(r => (r.getCrawlDate.substring(0, 6), ExtractLinks(r.getUrl, r.getContentString)))
      .flatMap(r => r._2.map(f => (r._1, f._1, f._2)))
      .filter(r => r._2 != null && r._3 != null)
      .map(_._1)
      .countItems()
    println(linkCounts.take(1).mkString)
  }

  def numPagesByDomain(sc: SparkContext): Unit = {
    val a = RecordLoader.loadArc(arcPath, sc)
    val b = RecordLoader.loadWarc(warcPath, sc)
    val domainCounts = a.union(b)
      .keepValidPages()
      .map(r => (r.getCrawlDate.substring(0, 6), r.getDomain.removePrefixWWW()))
      .countItems()
      .filter(f => f._2 > 10)
      .sortBy(f => (f._1, f._2))
      .collect()
    println(domainCounts.take(1).mkString)
  }

  def linkStructure(sc: SparkContext) = {
    val linkStructure = RecordLoader.loadArc(arcPath, sc)
      .keepValidPages()
      .map(r => (r.getCrawlDate.substring(0, 6), ExtractLinks(r.getUrl, r.getContentString)))
      .flatMap(r => r._2.map(f => (r._1, ExtractDomain(f._1).removePrefixWWW(), ExtractDomain(f._2).removePrefixWWW())))
      .filter(r => r._2 != null && r._3 != null)
      .countItems()
      .collect()
    println(linkStructure.take(1).mkString)
  }

  def warclinkStructure(sc: SparkContext) = {
    val linkStructure = RecordLoader.loadWarc(warcPath, sc)
      .keepValidPages()
      .map(r => (ExtractDate(r.getCrawlDate, ExtractDate.DateComponent.YYYYMM), ExtractLinks(r.getUrl, r.getContentString)))
      .flatMap(r => r._2.map(f => (r._1, ExtractDomain(f._1).removePrefixWWW(), ExtractDomain(f._2).removePrefixWWW())))
      .filter(r => r._2 != null && r._3 != null)
      .countItems()
      .map(r => TupleFormatter.tabDelimit(r))
      .collect()
    println(linkStructure.take(1).head)
  }

  def main(args: Array[String]) = {
    val master = "local[4]"
    val appName = "example-spark"
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    val sc = new SparkContext(conf)
    try {
      numPagesPerCrawl(sc)
      numLinksPerCrawl(sc)
      numPagesByDomain(sc)
      linkStructure(sc)
      warclinkStructure(sc)
    } finally {
      sc.stop()
    }
  }
}
