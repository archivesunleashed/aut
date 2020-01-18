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

package io

import java.security.MessageDigest
import java.util.Base64

import io.archivesunleashed.data.ArchiveRecordWritable.ArchiveFormat
import io.archivesunleashed.data.{ArchiveRecordInputFormat, ArchiveRecordWritable}

import ArchiveRecordWritable.ArchiveFormat
import io.archivesunleashed.df.{DetectLanguageDF, DetectMimeTypeTikaDF, ExtractDateDF, ExtractDomainDF, RemoveHTMLDF}

import io.archivesunleashed.matchbox.{DetectLanguageRDD, DetectMimeTypeTika, ExtractDateRDD,
                                      ExtractDomainRDD, ExtractImageDetails, ExtractImageLinksRDD,
                                      ExtractLinksRDD, GetExtensionMimeRDD, RemoveHTMLRDD, RemoveHTTPHeaderRDD}
import io.archivesunleashed.matchbox.ExtractDateRDD.DateComponent
import io.archivesunleashed.matchbox.ExtractDateRDD.DateComponent.DateComponent
import java.net.URI
import java.net.URL
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{RangePartitioner, SerializableWritable, SparkContext}
import scala.reflect.ClassTag
import scala.util.matching.Regex

/**
  * Package object which supplies implicits to augment generic RDDs with AUT-specific transformations.
  */
package object archivesunleashed {
  /** Loads records from either WARCs or ARCs. */
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
      val uri = new URI(path)
      val fs = FileSystem.get(uri, sc.hadoopConfiguration)
      val p = new Path(path)
      sc.newAPIHadoopFile(getFiles(p, fs), classOf[ArchiveRecordInputFormat], classOf[LongWritable], classOf[ArchiveRecordWritable])
        .filter(r => (r._2.getFormat == ArchiveFormat.ARC) ||
          ((r._2.getFormat == ArchiveFormat.WARC) && r._2.getRecord.getHeader.getHeaderValue("WARC-Type").equals("response")))
        .map(r => new ArchiveRecordImpl(new SerializableWritable(r._2)))
    }
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
    * A Wrapper class around DF to allow Dfs of type ARCRecord and WARCRecord to be queried via a fluent API.
    *
    * To load such an DF, please use [[RecordLoader]] and apply .all() on it.
    */
  implicit class WARecordDF(df: DataFrame) extends java.io.Serializable {

    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    /** Removes all non-html-based data (images, executables, etc.) from html text. */
    def keepValidPagesDF(): DataFrame = {
      df.filter($"crawl_date" isNotNull)
        .filter(!($"url".rlike(".*robots\\.txt$")) &&
                  ( $"mime_type_web_server".rlike("text/html") ||
                    $"mime_type_web_server".rlike("application/xhtml+xml") ||
                    $"url".rlike("(?i).*htm$") ||
                    $"url".rlike("(?i).*html$")
                  )
               )
        .filter($"http_status_code" === 200)
    }

    /** Filters ArchiveRecord MimeTypes (web server).
      *
      * @param mimeTypes a list of Mime Types
      */
    def discardMimeTypesDF(mimeTypes: Set[String]): DataFrame = {
      val filteredMimeType = udf((mimeType: String) => !mimeTypes.contains(mimeType))
      df.filter(filteredMimeType($"mime_type_web_server"))
    }

    /** Filters detected dates.
      *
      * @param date a list of dates
      */
    def discardDateDF(date: String): DataFrame = {
      val filteredDate = udf((date_ : String) => date_ != date)
      df.filter(filteredDate($"crawl_date"))
    }

    /** Filters detected URLs.
      *
      * @param urls a list of urls
      */
    def discardUrlsDF(urls: Set[String]): DataFrame = {
      val filteredUrls = udf((url: String) => !urls.contains(url))
      df.filter(filteredUrls($"url"))
    }

    /** Filters detected domains.
      *
      * @param domains a list of domains for the source domains
      */
    def discardDomainsDF(domains: Set[String]): DataFrame = {
      val filteredDomains = udf((domain: String) => !domains.contains(domain))
      df.filter(filteredDomains(ExtractDomainDF($"url")))
    }

    /** Filters detected HTTP status codes.
      *
      * @param statusCodes a list of HTTP status codes
      */
    def discardHttpStatusDF(statusCodes: Set[String]): DataFrame = {
      val filteredHttpStatus = udf((statusCode: String) => !statusCodes.contains(statusCode))
      df.filter(filteredHttpStatus($"http_status_code"))
    }

    /** Filters detected content (regex).
      *
      * @param contentREs a list of regular expressions
      */
    def discardContentDF(contentREs: Set[Regex]): DataFrame = {
      val filteredContent = udf((c: String) => {
                          !contentREs.map(re =>
                          (re findFirstIn c) match {
                            case Some(v) => true
                            case None => false
                          }).exists(identity)
                        })
      df.filter(filteredContent($"content"))
    }

    /** Filters detected URL patterns (regex).
     *
     *  @param urlREs a list of Regular expressions
     */
    def discardUrlPatternsDF(urlREs: Set[Regex]): DataFrame = {
      val filteredUrlPatterns = udf((urlPattern: String) => {
                              !urlREs.map(re =>
                                urlPattern match {
                                  case re() => true
                                  case _ => false
                              }).exists(identity)
                            })
      df.filter(filteredUrlPatterns($"url"))
    }

    /** Filters detected language.
      *
      * @param lang a set of ISO 639-2 codes
      */
    def discardLanguagesDF(lang: Set[String]): DataFrame = {
      val filteredLanguage = udf((language: String) => !lang.contains(language))
      df.filter(filteredLanguage(DetectLanguageDF(RemoveHTMLDF($"content"))))
    }

    /** Removes all data except images. */
    def keepImagesDF(): DataFrame = {
      val takeImages = udf((date: String, mimeType: String) => date != null && mimeType.startsWith("image/"))
      df.filter(takeImages($"crawl_date", DetectMimeTypeTikaDF($"bytes")))
    }

    /** Removes all data that does not have selected HTTP status codes.
     *
     *  @param statusCodes a list of HTTP status codes
     */
    def keepHttpStatusDF(statusCodes: Set[String]): DataFrame = {
      val takeHttpStatus = udf((statusCode: String) => statusCodes.contains(statusCode))
      df.filter(takeHttpStatus($"http_status_code"))
    }

    /** Removes all data that does not have selected date.
      *
      * @param dates a list of dates
      * @param component the selected DateComponent string
      */
    def keepDateDF(dates: List[String], component: String = "YYYYMMDD"): DataFrame = {
      val takeDate = udf((date : String) => dates.contains(date))
      df.filter(takeDate(ExtractDateDF($"crawl_date",lit(component))))
    }

    /** Removes all data but selected exact URLs.
      *
      * @param urls a list of URLs to keep
      */
    def keepUrlsDF(urls: Set[String]): DataFrame = {
      val takeUrls = udf((url: String) => urls.contains(url))
      df.filter(takeUrls($"url"))
    }

    /** Removes all data but selected source domains.
      *
      * @param urls a list of urls for the source domains
      */
    def keepDomainsDF(domains: Set[String]): DataFrame = {
      val takeDomains = udf((domain: String) => domains.contains(domain))
      df.filter(takeDomains(ExtractDomainDF($"url")))
    }

    /** Removes all data but selected mimeTypeTikas specified.
      *
      * @param mimeTypesTika a list of Mime Types Tika
      */
    def keepMimeTypesTikaDF(mimeTypes: Set[String]): DataFrame = {
      val takeMimeTypeTika = udf((mimeTypeTika: String) => mimeTypes.contains(mimeTypeTika))
      df.filter(takeMimeTypeTika(DetectMimeTypeTikaDF($"bytes")))
    }

    /** Removes all data but selected mimeTypes specified.
      *
      * @param mimeTypes a list of Mime Types
      */
    def keepMimeTypesDF(mimeTypes: Set[String]): DataFrame = {
      val takeMimeType = udf((mimeType: String) => mimeTypes.contains(mimeType))
      df.filter(takeMimeType($"mime_type_web_server"))
    }

    /** Removes all content that does not pass Regular Expression test.
      *
      * @param contentREs a list of regular expressions to keep
      */
    def keepContentDF(contentREs: Set[Regex]): DataFrame = {
      val takeContent = udf((c: String) => {
                          contentREs.map(re =>
                            (re findFirstIn c) match {
                              case Some(v) => true
                              case None => false
                          }).exists(identity)
                        })
      df.filter(takeContent($"content"))
    }

    /** Removes all data but selected URL patterns.
      *
      * @param urlREs a list of regular expressions
      */
    def keepUrlPatternsDF(urlREs: Set[Regex]): DataFrame = {
      val takeUrlPatterns = udf((urlPattern: String) => {
                              urlREs.map(re =>
                                urlPattern match {
                                  case re() => true
                                  case _ => false
                              }).exists(identity)
                            })
      df.filter(takeUrlPatterns($"url"))
    }

    /** Removes all data not in selected language.
      *
      * @param lang a set of ISO 639-2 codes
      */
    def keepLanguagesDF(lang: Set[String]): DataFrame = {
      val takeLanguage = udf((language: String) => lang.contains(language))
      df.filter(takeLanguage((DetectLanguageDF(RemoveHTMLDF($"content")))))
    }
  }

  /**
    * A Wrapper class around RDD to allow RDDs of type ARCRecord and WARCRecord to be queried via a fluent API.
    *
    * To load such an RDD, please see [[RecordLoader]].
    */
  implicit class WARecordRDD(rdd: RDD[ArchiveRecord]) extends java.io.Serializable {

    /* Creates a column for Bytes as well in Dataframe.
       Call KeepImages OR KeepValidPages on RDD depending upon the requirement before calling this method */
    def all(): DataFrame = {
      val records = rdd.map(r => Row(r.getCrawlDate, r.getUrl, r.getMimeType,
          DetectMimeTypeTika(r.getBinaryBytes), r.getContentString,
          r.getBinaryBytes, r.getHttpStatus, r.getArchiveFilename))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("content", StringType, true))
        .add(StructField("bytes", BinaryType, true))
        .add(StructField("http_status_code", StringType, true))
        .add(StructField("archive_filename", StringType, true))

      val sqlContext = SparkSession.builder()
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /** Removes all non-html-based data (images, executables, etc.) from html text. */
    def keepValidPages(): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        r.getCrawlDate != null
          && (r.getMimeType == "text/html"
          || r.getMimeType == "application/xhtml+xml"
          || r.getUrl.toLowerCase.endsWith("htm")
          || r.getUrl.toLowerCase.endsWith("html"))
          && !r.getUrl.toLowerCase.endsWith("robots.txt")
          && r.getHttpStatus == "200")
    }

    /** Extracts webpages with columns for crawl data, url, MIME type, and content. */
    def webpages(): DataFrame = {
      val records = rdd.keepValidPages()
        .map(r => Row(r.getCrawlDate, r.getUrl, r.getMimeType,
          DetectMimeTypeTika(r.getBinaryBytes),
          DetectLanguageRDD(RemoveHTMLRDD(RemoveHTTPHeaderRDD(r.getContentString))),
          r.getContentString))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("language", StringType, true))
        .add(StructField("content", StringType, true))

      val sqlContext = SparkSession.builder()
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /** Extracts a webgraph with columns for crawl date, source url, destination url, and anchor text. */
    def webgraph(): DataFrame = {
      val records = rdd
        .keepValidPages()
        .flatMap(r => ExtractLinksRDD(r.getUrl, r.getContentString)
          .map(t => (r.getCrawlDate, t._1, t._2, t._3)))
        .filter(t => t._2 != "" && t._3 != "")
        .map(t => Row(t._1, t._2, t._3, t._4))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("src", StringType, true))
        .add(StructField("dest", StringType, true))
        .add(StructField("anchor", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extracts all the images links from a source page. */
    def imageLinks(): DataFrame = {
      val records = rdd
        .keepValidPages()
        .flatMap(r => ({
          val src = r.getUrl
          val imageUrls = ExtractImageLinksRDD(src, r.getContentString)
          imageUrls.map(url => (src, url))
        })
          .map(t => (r.getCrawlDate, t._1, t._2)))
        .map(t => Row(t._1, t._2, t._3))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("src", StringType, true))
        .add(StructField("image_url", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract image bytes and image metadata. */
    def images(): DataFrame = {
      val records = rdd
        .keepImages()
        .map(r => {
          val mimeTypeTika = DetectMimeTypeTika(r.getBinaryBytes)
          val image = ExtractImageDetails(r.getUrl, mimeTypeTika, r.getBinaryBytes)
          val url = new URL(r.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMimeRDD(url.getPath(), mimeTypeTika)
          (r.getCrawlDate, r.getUrl, filename, extension, r.getMimeType, mimeTypeTika,
            image.width, image.height, image.md5Hash, image.sha1Hash, image.body)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("width", IntegerType, true))
        .add(StructField("height", IntegerType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("bytes", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract PDF bytes and PDF metadata. */
    def pdfs(): DataFrame = {
      val records = rdd
        .map(r =>
            (r, (DetectMimeTypeTika(r.getBinaryBytes)))
            )
        .filter(r => r._2 == "application/pdf")
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes)))
          val sha1Hash = new String(Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes)))
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMimeRDD(url.getPath(), r._2)
          (r._1.getCrawlDate, r._1.getUrl, filename, extension, r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("bytes", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract audio bytes and audio metadata. */
    def audio(): DataFrame = {
      val records = rdd
        .map(r =>
            (r, (DetectMimeTypeTika(r.getBinaryBytes)))
            )
        .filter(r => r._2.startsWith("audio/"))
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes)))
          val sha1Hash = new String(Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes)))
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMimeRDD(url.getPath(), r._2)
          (r._1.getCrawlDate, r._1.getUrl, filename, extension, r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("bytes", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract video bytes and video metadata. */
    def videos(): DataFrame = {
      val records = rdd
        .map(r =>
            (r, (DetectMimeTypeTika(r.getBinaryBytes)))
            )
        .filter(r => r._2.startsWith("video/"))
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes)))
          val sha1Hash = new String(Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes)))
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMimeRDD(url.getPath(), r._2)
          (r._1.getCrawlDate, r._1.getUrl, filename, extension, r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("bytes", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract spreadsheet bytes and spreadsheet metadata. */
    def spreadsheets(): DataFrame = {
      val records = rdd
        .map(r =>
            (r, (DetectMimeTypeTika(r.getBinaryBytes)))
            )
        .filter(r => (r._2 == "application/vnd.ms-excel"
          || r._2 == "application/vnd.ms-excel.workspace.3"
          || r._2 == "application/vnd.ms-excel.workspace.4"
          || r._2 == "application/vnd.ms-excel.sheet.2"
          || r._2 == "application/vnd.ms-excel.sheet.3"
          || r._2 == "application/vnd.ms-excel.sheet.3"
          || r._2 == "application/vnd.ms-excel.addin.macroenabled.12"
          || r._2 == "application/vnd.ms-excel.sheet.binary.macroenabled.12"
          || r._2 == "application/vnd.ms-excel.sheet.macroenabled.12"
          || r._2 == "application/vnd.ms-excel.template.macroenabled.12"
          || r._2 == "application/vnd.ms-spreadsheetml"
          || r._2 == "application/vnd.openxmlformats-officedocument.spreadsheetml.template"
          || r._2 == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
          || r._2 == "application/x-vnd.oasis.opendocument.spreadsheet-template"
          || r._2 == "application/vnd.oasis.opendocument.spreadsheet-template"
          || r._2 == "application/vnd.oasis.opendocument.spreadsheet"
          || r._2 == "application/x-vnd.oasis.opendocument.spreadsheet"
          || r._2 == "application/x-tika-msworks-spreadsheet"
          || r._2 == "application/vnd.lotus-1-2-3"
          || r._2 == "text/csv"                  // future versions of Tika?
          || r._2 == "text/tab-separated-values" // " "
          || r._1.getMimeType == "text/csv"
          || r._1.getMimeType == "text/tab-separated-values")
          || ((r._1.getUrl.toLowerCase.endsWith(".csv")
            || r._1.getUrl.toLowerCase.endsWith(".tsv"))
            && r._2 == "text/plain"))
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes)))
          val sha1Hash = new String(Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes)))
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          var mimeType = r._2
          if (mimeType == "text/plain") {
            if (r._1.getUrl.toLowerCase.endsWith(".csv")) {
              mimeType = "test/csv"
            } else if (r._1.getUrl.toLowerCase.endsWith(".tsv")) {
              mimeType = "text/tab-separated-values"
            }
          }
          val extension = GetExtensionMimeRDD(url.getPath(), mimeType)
          (r._1.getCrawlDate, r._1.getUrl, filename, extension, r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("bytes", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract presentation program bytes and presentation program metadata. */
    def presentationProgramFiles(): DataFrame = {
      val records = rdd
        .map(r =>
            (r, (DetectMimeTypeTika(r.getBinaryBytes)))
            )
        .filter(r => r._2 == "application/vnd.ms-powerpoint"
          || r._2 == "application/vnd.openxmlformats-officedocument.presentationml.presentation"
          || r._2 == "application/vnd.oasis.opendocument.presentation"
          || r._2 == "application/vnd.oasis.opendocument.presentation-template"
          || r._2 == "application/vnd.sun.xml.impress"
          || r._2 == "application/vnd.sun.xml.impress.template"
          || r._2 == "application/vnd.stardivision.impress"
          || r._2 == "application/x-starimpress"
          || r._2 == "application/vnd.ms-powerpoint.addin.macroEnabled.12"
          || r._2 == "application/vnd.ms-powerpoint.presentation.macroEnabled.12"
          || r._2 == "application/vnd.ms-powerpoint.slide.macroEnabled.12"
          || r._2 == "application/vnd.ms-powerpoint.slideshow.macroEnabled.12"
          || r._2 == "application/vnd.ms-powerpoint.template.macroEnabled.12")
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes)))
          val sha1Hash = new String(Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes)))
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMimeRDD(url.getPath(), r._2)
          (r._1.getCrawlDate, r._1.getUrl, filename, extension, r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("bytes", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract word processor bytes and word processor metadata. */
    def wordProcessorFiles(): DataFrame = {
      val records = rdd
        .map(r =>
            (r, (DetectMimeTypeTika(r.getBinaryBytes)))
            )
        .filter(r => r._2 == "application/vnd.lotus-wordpro"
          || r._2 == "application/vnd.kde.kword"
          || r._2 == "application/vnd.ms-word.document.macroEnabled.12"
          || r._2 == "application/vnd.ms-word.template.macroEnabled.12"
          || r._2 == "application/vnd.oasis.opendocument.text"
          || r._2 == "application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml"
          || r._2 == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
          || r._2 == "application/vnd.openxmlformats-officedocument.wordprocessingml.document.glossary+xml"
          || r._2 == "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"
          || r._2 == "application/vnd.wordperfect"
          || r._2 == "application/wordperfect5.1"
          || r._2 == "application/msword"
          || r._2 == "application/vnd.ms-word.document.macroEnabled.12"
          || r._2 == "application/vnd.ms-word.template.macroEnabled.12"
          || r._2 == "application/vnd.apple.pages"
          || r._2 == "application/macwriteii"
          || r._2 == "application/vnd.ms-works"
          || r._2 == "application/rtf")
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes)))
          val sha1Hash = new String(Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes)))
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMimeRDD(url.getPath(), r._2)
          (r._1.getCrawlDate, r._1.getUrl, filename, extension, r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("bytes", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract plain text bytes and plain text metadata. */
    def textFiles(): DataFrame = {
      val records = rdd
        .keepMimeTypes(Set("text/plain"))
        .filter(r => r.getMimeType == "text/plain"
          && (!r.getUrl.toLowerCase.endsWith("robots.txt")
            && !r.getUrl.toLowerCase.endsWith(".js")
            && !r.getUrl.toLowerCase.endsWith(".css")
            && !r.getUrl.toLowerCase.endsWith(".htm")
            && !r.getUrl.toLowerCase.endsWith(".html")
            && !r.getUrl.toLowerCase.startsWith("dns:")
            && !r.getUrl.toLowerCase.startsWith("filedesc:"))
        )
        .map(r => {
          val bytes = r.getBinaryBytes
          val md5Hash = new String(Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes)))
          val sha1Hash = new String(Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes)))
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = FilenameUtils.getExtension(url.getPath())
          (r.getCrawlDate, r.getUrl, filename, extension, r.getMimeType,
            DetectMimeTypeTika(r.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("bytes", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /** Removes all data except images. */
    def keepImages(): RDD[ArchiveRecord] = {
      rdd.filter(r => r.getCrawlDate != null
        && DetectMimeTypeTika(r.getBinaryBytes).startsWith("image/"))
    }

    /** Removes all data but selected mimeTypes specified.
      *
      * @param mimeTypes a list of Mime Types
      */
    def keepMimeTypes(mimeTypes: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => mimeTypes.contains(r.getMimeType))
    }

    /** Removes all data but selected mimeTypes as detected by Tika.
      *
      * @param mimeTypes a list of Mime Types
      */
    def keepMimeTypesTika(mimeTypes: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => mimeTypes.contains(DetectMimeTypeTika(r.getBinaryBytes)))
    }

    /** Removes all data that does not have selected HTTP status codes.
     *
     *  @param statusCodes a list of HTTP status codes
     */
    def keepHttpStatus(statusCodes: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => statusCodes.contains(r.getHttpStatus))
    }

    /** Removes all data that does not have selected date.
      *
      * @param dates a list of dates
      * @param component the selected DateComponent enum value
      */
    def keepDate(dates: List[String], component: DateComponent = DateComponent.YYYYMMDD): RDD[ArchiveRecord] = {
      rdd.filter(r => dates.contains(ExtractDateRDD(r.getCrawlDate, component)))
    }

    /** Removes all data but selected exact URLs.
      *
      * @param urls a list of URLs to keep
      */
    def keepUrls(urls: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => urls.contains(r.getUrl))
    }

    /** Removes all data but selected URL patterns.
      *
      * @param urlREs a list of regular expressions
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
      * @param urls a list of urls for the source domains
      */
    def keepDomains(urls: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => urls.contains(ExtractDomainRDD(r.getUrl).replace("^\\s*www\\.", "")))
    }

    /** Removes all data not in selected language.
      *
      * @param lang a set of ISO 639-2 codes
      */
    def keepLanguages(lang: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => lang.contains(DetectLanguageRDD(RemoveHTMLRDD(r.getContentString))))
    }

    /** Removes all content that does not pass Regular Expression test.
      *
      * @param contentREs a list of regular expressions to keep
      */
    def keepContent(contentREs: Set[Regex]): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        contentREs.map(re =>
          (re findFirstIn r.getContentString) match {
            case Some(v) => true
            case None => false
          }).exists(identity))
    }

    /** Filters ArchiveRecord MimeTypes (web server).
      *
      * @param mimeTypes a list of Mime Types
      */
    def discardMimeTypes(mimeTypes: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => !mimeTypes.contains(r.getMimeType))
    }

    /** Filters detected MimeTypes (Tika).
      *
      * @param mimeTypes a list of Mime Types
      */
    def discardMimeTypesTika(mimeTypes: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => !mimeTypes.contains(DetectMimeTypeTika(r.getBinaryBytes)))
    }

    /** Filters detected dates.
      *
      * @param date a list of dates
      */
    def discardDate(date: String): RDD[ArchiveRecord] = {
      rdd.filter(r => r.getCrawlDate != date)
    }

    /** Filters detected URLs.
      *
      * @param urls a list of urls
      */
    def discardUrls(urls: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => !urls.contains(r.getUrl))
    }

    /** Filters detected HTTP status codes.
      *
      * @param statusCodes a list of HTTP status codes
      */
    def discardHttpStatus(statusCodes: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => !statusCodes.contains(r.getHttpStatus))
    }

    /** Filters detected URL patterns (regex).
     *
     *  @param urlREs a list of Regular expressions
     */
    def discardUrlPatterns(urlREs: Set[Regex]): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        !urlREs.map(re =>
          r.getUrl match {
            case re() => true
            case _ => false
          }).exists(identity))
    }

    /** Filters detected domains (regex).
      *
      * @param urls a list of urls for the source domains
      */
    def discardDomains(urls: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => !urls.contains(r.getDomain))
    }

    /** Filters detected content (regex).
      *
      * @param contentREs a list of regular expressions
      */
    def discardContent(contentREs: Set[Regex]): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        !contentREs.map(re =>
          (re findFirstIn r.getContentString) match {
            case Some(v) => true
            case None => false
          }).exists(identity))
    }

    /** Filters detected language.
      *
      * @param lang a set of ISO 639-2 codes
      */
    def discardLanguages(lang: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => !lang.contains(DetectLanguageRDD(RemoveHTMLRDD(r.getContentString))))
    }
  }
}
