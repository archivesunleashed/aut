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

import io.archivesunleashed.data.{ArchiveRecordInputFormat, ArchiveRecordWritable}
import ArchiveRecordWritable.ArchiveFormat
import io.archivesunleashed.matchbox.{DetectLanguage, DetectMimeTypeTika, ExtractDate, ExtractDomain, ExtractImageDetails, ExtractImageLinks, ExtractLinks, GetExtensionMime, RemoveHTML}
import io.archivesunleashed.matchbox.ExtractDate.DateComponent
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import io.archivesunleashed.matchbox.ExtractDate.DateComponent.DateComponent
import java.net.URI
import java.net.URL
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.{SerializableWritable, SparkContext}
import org.apache.spark.rdd.RDD
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
    * A Wrapper class around RDD to allow RDDs of type ARCRecord and WARCRecord to be queried via a fluent API.
    *
    * To load such an RDD, please see [[RecordLoader]].
    */
  implicit class WARecordRDD(rdd: RDD[ArchiveRecord]) extends java.io.Serializable {
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

    def pages(): DataFrame = {
      val records = rdd.keepValidPages()
        .map(r => Row(r.getCrawlDate, r.getUrl, r.getMimeType,
          DetectMimeTypeTika(r.getBinaryBytes), r.getContentString))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("content", StringType, true))

      val sqlContext = SparkSession.builder()
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    def webgraph(): DataFrame = {
      val records = rdd
        .keepValidPages()
        .flatMap(r => ExtractLinks(r.getUrl, r.getContentString)
        .map(t => (r.getCrawlDate, t._1, t._2, t._3)))
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

    /* Extract image bytes and image metadata. */
    def images(): DataFrame = {
      val records = rdd
        .keepImages()
        .map(r => {
          val mimeTypeTika = DetectMimeTypeTika(r.getBinaryBytes)
          val image = ExtractImageDetails(r.getUrl, mimeTypeTika, r.getBinaryBytes)
          val url = new URL(r.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMime(url.getPath(), mimeTypeTika)
          (r.getUrl, filename, extension, r.getMimeType, mimeTypeTika,
            image.width, image.height, image.md5Hash, image.sha1Hash, image.body)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10))

      val schema = new StructType()
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
          val extension = GetExtensionMime(url.getPath(), r._2)
          (r._1.getUrl, filename, extension, r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8))

      val schema = new StructType()
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
          val extension = GetExtensionMime(url.getPath(), r._2)
          (r._1.getUrl, filename, extension, r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8))

      val schema = new StructType()
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
          val extension = GetExtensionMime(url.getPath(), r._2)
          (r._1.getUrl, filename, extension, r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8))

      val schema = new StructType()
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
          val extension = GetExtensionMime(url.getPath(), mimeType)
          (r._1.getUrl, filename, extension, r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8))

      val schema = new StructType()
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
          val extension = GetExtensionMime(url.getPath(), r._2)
          (r._1.getUrl, filename, extension, r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8))

      val schema = new StructType()
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
          val extension = GetExtensionMime(url.getPath(), r._2)
          (r._1.getUrl, filename, extension, r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8))

      val schema = new StructType()
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
        .filter(r => r.getUrl.toLowerCase.endsWith(".txt")
          || !r.getUrl.toLowerCase.endsWith("robots.txt")
          || !r.getUrl.toLowerCase.endsWith(".js")
          || !r.getUrl.toLowerCase.endsWith(".css")
          || !r.getUrl.toLowerCase.endsWith(".htm")
          || !r.getUrl.toLowerCase.endsWith(".html"))
        .map(r => {
          val bytes = r.getBinaryBytes
          val md5Hash = new String(Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes)))
          val sha1Hash = new String(Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes)))
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = FilenameUtils.getExtension(url.getPath())
          (r.getUrl, filename, extension, r.getMimeType,
            DetectMimeTypeTika(r.getBinaryBytes), md5Hash, sha1Hash, encodedBytes)
        })
        .map(t => Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8))

      val schema = new StructType()
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
      rdd.filter(r => dates.contains(ExtractDate(r.getCrawlDate, component)))
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
      rdd.filter(r => urls.contains(ExtractDomain(r.getUrl).replace("^\\s*www\\.", "")))
    }

    /** Removes all data not in selected language.
      *
      * @param lang a set of ISO 639-2 codes
      */
    def keepLanguages(lang: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r => lang.contains(DetectLanguage(RemoveHTML(r.getContentString))))
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
      rdd.filter(r => !lang.contains(DetectLanguage(RemoveHTML(r.getContentString))))
    }
  }
}
