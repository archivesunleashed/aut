package io

import io.archivesunleashed.io.ArchiveRecordWritable
import io.archivesunleashed.io.ArchiveRecordWritable.ArchiveFormat
import io.archivesunleashed.mapreduce.WacInputFormat
import io.archivesunleashed.matchbox.{DetectLanguage, ExtractDate, ExtractDomain, RemoveHTML}
import io.archivesunleashed.matchbox.ExtractDate.DateComponent
import io.archivesunleashed.matchbox.ExtractDate.DateComponent._
import org.apache.hadoop.io.LongWritable
import org.apache.spark.{SerializableWritable, SparkContext}
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.reflect.ClassTag
import scala.util.matching.Regex

package object archivesunleashed {
  object RecordLoader {
    def loadArchives(path: String, sc: SparkContext): RDD[ArchiveRecord] =
      sc.newAPIHadoopFile(path, classOf[WacInputFormat], classOf[LongWritable], classOf[ArchiveRecordWritable])
        .filter(r => (r._2.getFormat == ArchiveFormat.ARC) ||
          ((r._2.getFormat == ArchiveFormat.WARC) && r._2.getRecord.getHeader.getHeaderValue("WARC-Type").equals("response")))
        .map(r => new ArchiveRecord(new SerializableWritable(r._2)))

    def loadTweets(path: String, sc: SparkContext): RDD[JValue] =
      sc.textFile(path).filter(line => !line.startsWith("{\"delete\":"))
        .map(line => try { parse(line) } catch { case e: Exception => null }).filter(x => x != null)
  }

  /**
    * A Wrapper class around RDD to simplify counting
    */
  implicit class CountableRDD[T: ClassTag](rdd: RDD[T]) extends java.io.Serializable {
    def countItems(): RDD[(T, Int)] = {
      rdd.map(r => (r, 1))
        .reduceByKey((c1, c2) => c1 + c2)
        .sortBy(f => f._2, ascending = false)
    }
  }

  /**
    * A Wrapper class around RDD to allow RDDs of type ARCRecord and WARCRecord to be queried via a fluent API.
    *
    * To load such an RDD, please see [[io.archivesunleashed.spark.matchbox.RecordLoader]]
    */
  implicit class WARecordRDD(rdd: RDD[ArchiveRecord]) extends java.io.Serializable {

    def keepValidPages(): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        r.getCrawlDate != null
          && (r.getMimeType == "text/html"
          || r.getMimeType == "application/xhtml+xml"
          || r.getUrl.endsWith("htm")
          || r.getUrl.endsWith("html"))
          && !r.getUrl.endsWith("robots.txt"))
    }

    def keepImages() = {
      rdd.filter(r =>
        r.getCrawlDate != null
          && (
          (r.getMimeType != null && r.getMimeType.contains("image/"))
            || r.getUrl.endsWith("jpg")
            || r.getUrl.endsWith("jpeg")
            || r.getUrl.endsWith("png"))
          && !r.getUrl.endsWith("robots.txt"))
    }

    def keepMimeTypes(mimeTypes: Set[String]) = {
      rdd.filter(r => mimeTypes.contains(r.getMimeType))
    }

    def keepDate(dates: List[String], component: DateComponent = DateComponent.YYYYMMDD) = {
      rdd.filter(r => dates.contains(ExtractDate(r.getCrawlDate, component)))
    }

    def keepUrls(urls: Set[String]) = {
      rdd.filter(r => urls.contains(r.getUrl))
    }

    def keepUrlPatterns(urlREs: Set[Regex]) = {
      rdd.filter(r =>
        urlREs.map(re =>
          r.getUrl match {
            case re() => true
            case _ => false
          }).exists(identity))
    }

    def keepDomains(urls: Set[String]) = {
      rdd.filter(r => urls.contains(ExtractDomain(r.getUrl).replace("^\\s*www\\.", "")))
    }

    def keepLanguages(lang: Set[String]) = {
      rdd.filter(r => lang.contains(DetectLanguage(RemoveHTML(r.getContentString))))
    }

    def keepContent(contentREs: Set[Regex]) = {
      rdd.filter(r =>
        contentREs.map(re =>
          (re findFirstIn r.getContentString) match {
            case Some(v) => true
            case None => false
          }).exists(identity))
    }

    def discardMimeTypes(mimeTypes: Set[String]) = {
      rdd.filter(r => !mimeTypes.contains(r.getMimeType))
    }

    def discardDate(date: String) = {
      rdd.filter(r => r.getCrawlDate != date)
    }

    def discardUrls(urls: Set[String]) = {
      rdd.filter(r => !urls.contains(r.getUrl))
    }

    def discardUrlPatterns(urlREs: Set[Regex]) = {
      rdd.filter(r =>
        !urlREs.map(re =>
          r.getUrl match {
            case re() => true
            case _ => false
          }).exists(identity))
    }

    def discardDomains(urls: Set[String]) = {
      rdd.filter(r => !urls.contains(r.getDomain))
    }

    def discardContent(contentREs: Set[Regex]) = {
      rdd.filter(r =>
        !contentREs.map(re =>
          (re findFirstIn r.getContentString) match {
            case Some(v) => true
            case None => false
          }).exists(identity))
    }
  }
}
