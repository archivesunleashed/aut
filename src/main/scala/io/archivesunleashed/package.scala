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

package io

import io.archivesunleashed.data.{ArchiveRecordInputFormat, ArchiveRecordWritable}
import ArchiveRecordWritable.ArchiveFormat
import io.archivesunleashed.matchbox.{ComputeMD5, DetectLanguage, ExtractDate, ExtractDomain, ExtractImageDetails, ExtractImageLinks, ExtractLinks, RemoveHTML}
import io.archivesunleashed.matchbox.ImageDetails
import io.archivesunleashed.matchbox.ExtractDate.DateComponent
import org.apache.hadoop.fs.{FileSystem, Path}
// scalastyle:off underscore.import
import io.archivesunleashed.matchbox.ExtractDate.DateComponent._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.jackson.JsonMethods._
// scalastyle:on: underscore.import
import org.apache.hadoop.io.LongWritable
import org.apache.spark.{SerializableWritable, SparkContext}
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.util.matching.Regex

/**
  * Package object which supplies implicits to augment generic RDDs with AUT-specific transformations.
  */
package object archivesunleashed {
  /** Loads records from either WARCs, ARCs or Twitter API data (JSON). */
  object RecordLoader {
    /** Gets all non-empty archive files.
      *
      * @param dir the path to the directory containing archive files
      * @param fs filesystem
      * @return a String consisting of all non-empty archive files path.
      */
    def getFiles(dir: Path, fs: FileSystem): String = {
      val statuses = fs.globStatus(dir)
      val files = statuses.filter(f => fs.getContentSummary(f.getPath).getLength > 0).map(f => f.getPath)
      files.mkString(",")
    }

    /** Creates an Archive Record RDD from a WARC or ARC file.
      *
      * @param path the path to the WARC(s)
      * @param sc the apache spark context
      * @return an RDD of ArchiveRecords for mapping.
      */
    def loadArchives(path: String, sc: SparkContext): RDD[ArchiveRecord] = {
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val p = new Path(path)
      sc.newAPIHadoopFile(getFiles(p, fs), classOf[ArchiveRecordInputFormat], classOf[LongWritable], classOf[ArchiveRecordWritable])
        .filter(r => (r._2.getFormat == ArchiveFormat.ARC) ||
          ((r._2.getFormat == ArchiveFormat.WARC) && r._2.getRecord.getHeader.getHeaderValue("WARC-Type").equals("response")))
        .map(r => new ArchiveRecordImpl(new SerializableWritable(r._2)))
    }

    /** Creates an Archive Record RDD from tweets.
      *
      * @param path the path to the Tweets file
      * @param sc the apache spark context
      * @return an RDD of JValue (json objects) for mapping.
      */
    def loadTweets(path: String, sc: SparkContext): RDD[JValue] =
      // scalastyle:off null
      sc.textFile(path).filter(line => !line.startsWith("{\"delete\":"))
        .map(line => try { parse(line) } catch { case e: Exception => null }).filter(x => x != null)
      // scalastyle:on null
  }

  /** A Wrapper class around RDD to simplify counting. */
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
    * To load such an RDD, please see [[RecordLoader]].
    */
  implicit class WARecordRDD(rdd: RDD[ArchiveRecord]) extends java.io.Serializable {
    /** Removes all non-html-based data (images, executables etc.) from html text. */
    def keepValidPages(): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        r.getCrawlDate != null
          && (r.getMimeType == "text/html"
          || r.getMimeType == "application/xhtml+xml"
          || r.getUrl.endsWith("htm")
          || r.getUrl.endsWith("html"))
          && !r.getUrl.endsWith("robots.txt"))
    }

    def extractValidPagesDF(): DataFrame = {
      val records = rdd.keepValidPages()
        .map(r => Row(r.getCrawlDate, r.getUrl, r.getMimeType, r.getContentString))

      val schema = new StructType()
        .add(StructField("CrawlDate", StringType, true))
        .add(StructField("Url", StringType, true))
        .add(StructField("MimeType", StringType, true))
        .add(StructField("Content", StringType, true))

      val sqlContext = SparkSession.builder()
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    def extractHyperlinksDF(): DataFrame = {
      val records = rdd
        .keepValidPages()
        .flatMap(r => ExtractLinks(r.getUrl, r.getContentString).map(t => (r.getCrawlDate, t._1, t._2, t._3)))
        .map(t => Row(t._1, t._2, t._3, t._4))

      val schema = new StructType()
        .add(StructField("CrawlDate", StringType, true))
        .add(StructField("Src", StringType, true))
        .add(StructField("Dest", StringType, true))
        .add(StructField("Anchor", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extracts all the images from a source page */
    def extractImageLinksDF(): DataFrame = {
      val records = rdd
        .keepValidPages()
        .flatMap(r => {
          val src = r.getUrl
          val imageUrls = ExtractImageLinks(src, r.getContentString)
          imageUrls.map(url => (src, url))
        })
        .map(t => Row(t._1, t._2))

      val schema = new StructType()
        .add(StructField("src", StringType, true))
        .add(StructField("image_url", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract image bytes and metadata */
    def extractImageDetailsDF(): DataFrame = {
      val records = rdd
        .keepImages()
        .map(r => {
          val image = ExtractImageDetails(r.getUrl, r.getMimeType, r.getImageBytes)
          (r.getUrl, r.getMimeType, image.width, image.height, image.hash, image.body)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6))

      val schema = new StructType()
        .add(StructField("url", StringType, true))
        .add(StructField("mime_type", StringType, true))
        .add(StructField("width", IntegerType, true))
        .add(StructField("height", IntegerType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("bytes", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /** Removes all data except images. */
    def keepImages(): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        r.getCrawlDate != null
          && (
          (r.getMimeType != null && r.getMimeType.contains("image/"))
            || r.getUrl.endsWith("jpg")
            || r.getUrl.endsWith("jpeg")
            || r.getUrl.endsWith("png"))
          && !r.getUrl.endsWith("robots.txt"))
    }

    /** Removes all data but selected mimeTypes.
      *
      * @param mimeTypes a Set of Mimetypes to keep
      */
    def keepMimeTypes(mimeTypes: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => mimeTypes.contains(r.getMimeType))
    }

    /** Removes all data that does not have selected data.
      *
      * @param dates a list of dates to keep
      * @param component the selected DateComponent enum value
      */
    def keepDate(dates: List[String], component: DateComponent = DateComponent.YYYYMMDD): RDD[ArchiveRecord] = {
      rdd.filter(r => dates.contains(ExtractDate(r.getCrawlDate, component)))
    }

    /** Removes all data but selected exact URLs
      *
      * @param urls a Set of URLs to keep
      */
    def keepUrls(urls: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => urls.contains(r.getUrl))
    }

    /** Removes all data but selected url patterns.
      *
      * @param urlREs a Set of Regular Expressions to keep
      */
    def keepUrlPatterns(urlREs: Set[Regex]): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        urlREs.map(re =>
          r.getUrl match {
            case re() => true
            case _ => false
          }).exists(identity))
    }

    /** Removes all data but selected source domains.
      *
      * @param urls a Set of urls for the source domains to keep
      */
    def keepDomains(urls: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => urls.contains(ExtractDomain(r.getUrl).replace("^\\s*www\\.", "")))
    }

    /** Removes all data not in selected language.
      *
      * @param lang a Set of ISO 639-2 codes
      */
    def keepLanguages(lang: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => lang.contains(DetectLanguage(RemoveHTML(r.getContentString))))
    }

    /** Removes all content that does not pass Regular Expression test.
      *
      * @param contentREs a list of Regular expressions to keep
      */
    def keepContent(contentREs: Set[Regex]): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        contentREs.map(re =>
          (re findFirstIn r.getContentString) match {
            case Some(v) => true
            case None => false
          }).exists(identity))
    }

    /** Filters MimeTypes from RDDs.
      *
      * @param mimeTypes
      */
    def discardMimeTypes(mimeTypes: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => !mimeTypes.contains(r.getMimeType))
    }

    def discardDate(date: String): RDD[ArchiveRecord] = {
      rdd.filter(r => r.getCrawlDate != date)
    }

    def discardUrls(urls: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => !urls.contains(r.getUrl))
    }

    def discardUrlPatterns(urlREs: Set[Regex]): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        !urlREs.map(re =>
          r.getUrl match {
            case re() => true
            case _ => false
          }).exists(identity))
    }

    def discardDomains(urls: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => !urls.contains(r.getDomain))
    }

    def discardContent(contentREs: Set[Regex]): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        !contentREs.map(re =>
          (re findFirstIn r.getContentString) match {
            case Some(v) => true
            case None => false
          }).exists(identity))
    }
  }
}
