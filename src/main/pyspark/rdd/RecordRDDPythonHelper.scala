package io.archivesunleashed.pyspark.rdd

import io.archivesunleashed.spark.archive.io.ArchiveRecord
import io.archivesunleashed.spark.matchbox.ExtractDate.DateComponent
import io.archivesunleashed.spark.matchbox.ExtractDate.DateComponent.DateComponent
import io.archivesunleashed.spark.matchbox.{DetectLanguage, ExtractDate, ExtractDomain, RemoveHTML}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.matching.Regex

object RecordRDDPythonHelper {
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
  implicit class WARecordRDD(jrdd: JavaRDD[ArchiveRecord]) extends java.io.Serializable {
    val rdd: RDD[ArchiveRecord] = jrdd.rdd

    def keepValidPages(): JavaRDD[ArchiveRecord] = {
      val rdd = jrdd.rdd
      new JavaRDD(rdd.filter(r =>
        r.getCrawlDate != null
          && (r.getMimeType == "text/html"
          || r.getMimeType == "application/xhtml+xml"
          || r.getUrl.endsWith("htm")
          || r.getUrl.endsWith("html"))
          && !r.getUrl.endsWith("robots.txt")))
    }

    def keepImages() = {
      val rdd = jrdd.rdd
      new JavaRDD(rdd.filter(r =>
        r.getCrawlDate != null
          && (
          (r.getMimeType != null && r.getMimeType.contains("image/"))
          || r.getUrl.endsWith("jpg")
          || r.getUrl.endsWith("jpeg")
          || r.getUrl.endsWith("png"))
          && !r.getUrl.endsWith("robots.txt")))
    }

    def keepMimeTypes(mimeTypes: Set[String]) = {
      val rdd = jrdd.rdd
      new JavaRDD(rdd.filter(r => mimeTypes.contains(r.getMimeType)))
    }

    def keepDate(date: String, component: DateComponent = DateComponent.YYYYMMDD) = {
      val rdd = jrdd.rdd
      rdd.filter(r => ExtractDate(r.getCrawlDate, component) == date)
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
