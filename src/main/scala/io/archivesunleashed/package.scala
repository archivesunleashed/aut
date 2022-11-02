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

import java.io.InputStream
import java.security.MessageDigest
import java.util.Base64

import io.archivesunleashed.matchbox.{
  CovertLastModifiedDate,
  DetectLanguage,
  DetectMimeTypeTika,
  ExtractDate,
  ExtractDomain,
  ExtractImageDetails,
  ExtractImageLinks,
  ExtractLinks,
  GetExtensionMIME,
  RemoveHTML,
  RemoveHTTPHeader
}
import io.archivesunleashed.matchbox.ExtractDate.DateComponent
import io.archivesunleashed.matchbox.ExtractDate.DateComponent.DateComponent
import java.net.URI
import java.net.URL

import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{lit, lower, udf}
import org.apache.spark.sql.types.{
  BinaryType,
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{RangePartitioner, SerializableWritable, SparkContext}
import org.archive.webservices.sparkling.io.{HdfsIO, IOUtil}
import org.archive.webservices.sparkling.util.{
  IteratorUtil,
  ManagedVal,
  RddUtil,
  ValueSupplier
}
import org.archive.webservices.sparkling.warc.{WarcLoader, WarcRecord}

import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.matching.Regex
import scala.util.Try

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
      val files = statuses
        .filter(f => fs.getContentSummary(f.getPath).getLength > 0)
        .map(f => f.getPath)
      files.mkString(",")
    }

    /** Creates an Archive Record RDD from a WARC or ARC file.
      *
      * @param path the path to the WARC(s)
      * @param sc the apache spark context
      * @return an RDD of ArchiveRecords for mapping.
      */
    def loadArchives(path: String, sc: SparkContext): RDD[ArchiveRecord] = {
      RddUtil.loadFilesLocality(path).flatMap { path =>
        val filename = path.split('/').last
        val in = HdfsIO.open(path, decompress = false)
        var prev: Option[ManagedVal[ValueSupplier[InputStream]]] = None
        IteratorUtil.cleanup(
          WarcLoader.load(in).filter(r => r.isResponse || r.isRevisit).map {
            record =>
              for (p <- prev) p.clear(false)
              val buffered = IOUtil.buffer(lazyEval = true) { out =>
                IOUtil.copy(record.payload, out)
              }
              prev = Some(buffered)
              new SparklingArchiveRecord(filename, record, buffered)
          },
          () => {
            for (p <- prev) p.clear(false)
            in.close()
          }
        )
      }
    }
  }

  /** A Wrapper class around RDD to simplify counting. */
  implicit class CountableRDD[T: ClassTag](rdd: RDD[T])
      extends java.io.Serializable {
    def countItems(): RDD[(T, Int)] = {
      rdd
        .map(r => (r, 1))
        .reduceByKey((c1, c2) => c1 + c2)
        .sortBy(f => f._2, ascending = false)
    }
  }

  /**
    * A Wrapper class around DF to allow Dfs of type ArchiveRecord to be queried via a fluent API.
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
        .filter(
          !($"url".rlike(".*robots\\.txt$")) &&
            ($"mime_type_web_server".rlike("text/html") ||
              $"mime_type_web_server".rlike("application/xhtml+xml") ||
              $"url".rlike("(?i).*htm$") ||
              $"url".rlike("(?i).*html$"))
        )
        .filter($"http_status_code" === 200)
    }
  }

  /**
    * A Wrapper class around RDD to allow RDDs of type ArchiveRecord to be queried via a fluent API.
    *
    * To load such an RDD, please see [[RecordLoader]].
    */
  implicit class WARecordRDD(rdd: RDD[ArchiveRecord])
      extends java.io.Serializable {

    /* Creates a column for Bytes as well in Dataframe.
       Call KeepImages OR KeepValidPages on RDD depending upon the requirement before calling this method. */
    def all(): DataFrame = {
      val records = rdd
        .removeFiledesc()
        .map(r =>
          Row(
            r.getCrawlDate,
            CovertLastModifiedDate(r.getLastModified),
            ExtractDomain(r.getUrl).replaceAll("^\\s*www\\.", ""),
            r.getUrl,
            r.getMimeType,
            DetectMimeTypeTika(r.getBinaryBytes),
            r.getContentString,
            r.getBinaryBytes,
            r.getHttpStatus,
            r.getArchiveFilename
          )
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
        .add(StructField("domain", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("raw_content", StringType, true))
        .add(StructField("bytes", BinaryType, true))
        .add(StructField("http_status_code", StringType, true))
        .add(StructField("archive_filename", StringType, true))

      val sqlContext = SparkSession.builder()
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /** Filters out filedesc:// and dns: records. */
    def removeFiledesc(): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        !r.getUrl.toLowerCase.startsWith("filedesc:")
          && !r.getUrl.toLowerCase.startsWith("dns:")
      )
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
          && r.getHttpStatus == "200"
      )
    }

    /** Extracts webpages with columns for crawl data, url, MIME type, and content. */
    def webpages(): DataFrame = {
      val records = rdd
        .removeFiledesc()
        .keepValidPages()
        .map(r =>
          Row(
            r.getCrawlDate,
            CovertLastModifiedDate(r.getLastModified),
            ExtractDomain(r.getUrl).replaceAll("^\\s*www\\.", ""),
            r.getUrl,
            r.getMimeType,
            DetectMimeTypeTika(r.getBinaryBytes),
            DetectLanguage(RemoveHTML(RemoveHTTPHeader(r.getContentString))),
            RemoveHTML(RemoveHTTPHeader(r.getContentString))
          )
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
        .add(StructField("domain", StringType, true))
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
        .removeFiledesc()
        .keepValidPages()
        .flatMap(r =>
          ExtractLinks(r.getUrl, r.getContentString)
            .map(t => (r.getCrawlDate, t._1, t._2, t._3))
        )
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
    def imagegraph(): DataFrame = {
      val records = rdd
        .removeFiledesc()
        .keepValidPages()
        .flatMap(r =>
          ExtractImageLinks(r.getUrl, r.getContentString)
            .map(t => (r.getCrawlDate, t._1, t._2, t._3))
        )
        .filter(t => t._2 != "" && t._3 != "")
        .map(t => Row(t._1, t._2, t._3, t._4))

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("src", StringType, true))
        .add(StructField("image_url", StringType, true))
        .add(StructField("alt_text", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract image bytes and image metadata. */
    def images(): DataFrame = {
      val records = rdd
        .keepImages()
        .map(r => {
          val mimeTypeTika = DetectMimeTypeTika(r.getBinaryBytes)
          val image =
            ExtractImageDetails(r.getUrl, mimeTypeTika, r.getBinaryBytes)
          val url = new URL(r.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMIME(url.getPath(), mimeTypeTika)
          (
            r.getCrawlDate,
            CovertLastModifiedDate(r.getLastModified),
            r.getUrl,
            filename,
            extension,
            r.getMimeType,
            mimeTypeTika,
            image.width,
            image.height,
            image.md5Hash,
            image.sha1Hash,
            image.body
          )
        })
        .map(t =>
          Row(
            t._1,
            t._2,
            t._3,
            t._4,
            t._5,
            t._6,
            t._7,
            t._8,
            t._9,
            t._10,
            t._11,
            t._12
          )
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
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
        .map(r => (r, (DetectMimeTypeTika(r.getBinaryBytes))))
        .filter(r => r._2 == "application/pdf")
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes))
          )
          val sha1Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes))
          )
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMIME(url.getPath(), r._2)
          (
            r._1.getCrawlDate,
            CovertLastModifiedDate(r._1.getLastModified),
            r._1.getUrl,
            filename,
            extension,
            r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes),
            md5Hash,
            sha1Hash,
            encodedBytes
          )
        })
        .map(t =>
          Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
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
        .map(r => (r, (DetectMimeTypeTika(r.getBinaryBytes))))
        .filter(r => r._2.startsWith("audio/"))
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes))
          )
          val sha1Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes))
          )
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMIME(url.getPath(), r._2)
          (
            r._1.getCrawlDate,
            CovertLastModifiedDate(r._1.getLastModified),
            r._1.getUrl,
            filename,
            extension,
            r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes),
            md5Hash,
            sha1Hash,
            encodedBytes
          )
        })
        .map(t =>
          Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
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
        .map(r => (r, (DetectMimeTypeTika(r.getBinaryBytes))))
        .filter(r => r._2.startsWith("video/"))
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes))
          )
          val sha1Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes))
          )
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMIME(url.getPath(), r._2)
          (
            r._1.getCrawlDate,
            CovertLastModifiedDate(r._1.getLastModified),
            r._1.getUrl,
            filename,
            extension,
            r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes),
            md5Hash,
            sha1Hash,
            encodedBytes
          )
        })
        .map(t =>
          Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
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
        .map(r => (r, (DetectMimeTypeTika(r.getBinaryBytes))))
        .filter(r =>
          (r._2 == "application/vnd.ms-excel"
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
            || r._2 == "text/csv" // future versions of Tika?
            || r._2 == "text/tab-separated-values" // " "
            || r._1.getMimeType == "text/csv"
            || r._1.getMimeType == "text/tab-separated-values")
            || ((r._1.getUrl.toLowerCase.endsWith(".csv")
              || r._1.getUrl.toLowerCase.endsWith(".tsv"))
              && r._2 == "text/plain")
        )
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes))
          )
          val sha1Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes))
          )
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
          val extension = GetExtensionMIME(url.getPath(), mimeType)
          (
            r._1.getCrawlDate,
            CovertLastModifiedDate(r._1.getLastModified),
            r._1.getUrl,
            filename,
            extension,
            r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes),
            md5Hash,
            sha1Hash,
            encodedBytes
          )
        })
        .map(t =>
          Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
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
        .map(r => (r, (DetectMimeTypeTika(r.getBinaryBytes))))
        .filter(r =>
          r._2 == "application/vnd.ms-powerpoint"
            || r._2 == "application/vnd.apple.keynote"
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
            || r._2 == "application/vnd.ms-powerpoint.template.macroEnabled.12"
        )
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes))
          )
          val sha1Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes))
          )
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMIME(url.getPath(), r._2)
          (
            r._1.getCrawlDate,
            CovertLastModifiedDate(r._1.getLastModified),
            r._1.getUrl,
            filename,
            extension,
            r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes),
            md5Hash,
            sha1Hash,
            encodedBytes
          )
        })
        .map(t =>
          Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
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
        .map(r => (r, (DetectMimeTypeTika(r.getBinaryBytes))))
        .filter(r =>
          r._2 == "application/vnd.lotus-wordpro"
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
            || r._2 == "application/rtf"
        )
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes))
          )
          val sha1Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes))
          )
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMIME(url.getPath(), r._2)
          (
            r._1.getCrawlDate,
            CovertLastModifiedDate(r._1.getLastModified),
            r._1.getUrl,
            filename,
            extension,
            r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes),
            md5Hash,
            sha1Hash,
            encodedBytes
          )
        })
        .map(t =>
          Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
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

    /* Extract css. */
    def css(): DataFrame = {
      val records = rdd
        .map(r => (r, (r.getMimeType)))
        .filter(r => r._2 == "text/css")
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes))
          )
          val sha1Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes))
          )
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMIME(url.getPath(), r._2)
          (
            r._1.getCrawlDate,
            CovertLastModifiedDate(r._1.getLastModified),
            r._1.getUrl,
            filename,
            extension,
            r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes),
            md5Hash,
            sha1Hash,
            RemoveHTTPHeader(r._1.getContentString)
          )
        })
        .map(t =>
          Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("content", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract html. */
    def html(): DataFrame = {
      val records = rdd
        .map(r => (r, (r.getMimeType)))
        .filter(r => r._2 == "text/html")
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes))
          )
          val sha1Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes))
          )
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMIME(url.getPath(), r._2)
          (
            r._1.getCrawlDate,
            CovertLastModifiedDate(r._1.getLastModified),
            r._1.getUrl,
            filename,
            extension,
            r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes),
            md5Hash,
            sha1Hash,
            RemoveHTTPHeader(r._1.getContentString)
          )
        })
        .map(t =>
          Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("content", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract javascript. */
    def js(): DataFrame = {
      val records = rdd
        .map(r => (r, (r.getMimeType)))
        .filter(r => r._2.contains("javascript"))
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes))
          )
          val sha1Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes))
          )
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMIME(url.getPath(), r._2)
          (
            r._1.getCrawlDate,
            CovertLastModifiedDate(r._1.getLastModified),
            r._1.getUrl,
            filename,
            extension,
            r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes),
            md5Hash,
            sha1Hash,
            RemoveHTTPHeader(r._1.getContentString)
          )
        })
        .map(t =>
          Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("content", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract json. */
    def json(): DataFrame = {
      val records = rdd
        .map(r => (r, (r.getMimeType)))
        .filter(r => r._2.contains("json"))
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes))
          )
          val sha1Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes))
          )
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMIME(url.getPath(), r._2)
          (
            r._1.getCrawlDate,
            CovertLastModifiedDate(r._1.getLastModified),
            r._1.getUrl,
            filename,
            extension,
            r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes),
            md5Hash,
            sha1Hash,
            RemoveHTTPHeader(r._1.getContentString)
          )
        })
        .map(t =>
          Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("content", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract plain text. */
    def plainText(): DataFrame = {
      val records = rdd
        .map(r => (r, (r.getMimeType), (DetectMimeTypeTika(r.getBinaryBytes))))
        .filter(r => r._2 == "text/plain")
        .filter(r => r._3 == "text/plain")
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes))
          )
          val sha1Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes))
          )
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMIME(url.getPath(), r._2)
          (
            r._1.getCrawlDate,
            CovertLastModifiedDate(r._1.getLastModified),
            r._1.getUrl,
            filename,
            extension,
            r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes),
            md5Hash,
            sha1Hash,
            RemoveHTTPHeader(r._1.getContentString)
          )
        })
        .map(t =>
          Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("content", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /* Extract xml. */
    def xml(): DataFrame = {
      val records = rdd
        .map(r => (r, (r.getMimeType)))
        .filter(r => r._2.contains("xml"))
        .map(r => {
          val bytes = r._1.getBinaryBytes
          val md5Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes))
          )
          val sha1Hash = new String(
            Hex.encodeHex(MessageDigest.getInstance("SHA1").digest(bytes))
          )
          val encodedBytes = Base64.getEncoder.encodeToString(bytes)
          val url = new URL(r._1.getUrl)
          val filename = FilenameUtils.getName(url.getPath())
          val extension = GetExtensionMIME(url.getPath(), r._2)
          (
            r._1.getCrawlDate,
            CovertLastModifiedDate(r._1.getLastModified),
            r._1.getUrl,
            filename,
            extension,
            r._1.getMimeType,
            DetectMimeTypeTika(r._1.getBinaryBytes),
            md5Hash,
            sha1Hash,
            RemoveHTTPHeader(r._1.getContentString)
          )
        })
        .map(t =>
          Row(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)
        )

      val schema = new StructType()
        .add(StructField("crawl_date", StringType, true))
        .add(StructField("last_modified", StringType, true))
        .add(StructField("url", StringType, true))
        .add(StructField("filename", StringType, true))
        .add(StructField("extension", StringType, true))
        .add(StructField("mime_type_web_server", StringType, true))
        .add(StructField("mime_type_tika", StringType, true))
        .add(StructField("md5", StringType, true))
        .add(StructField("sha1", StringType, true))
        .add(StructField("content", StringType, true))

      val sqlContext = SparkSession.builder();
      sqlContext.getOrCreate().createDataFrame(records, schema)
    }

    /** Removes all data except images. */
    def keepImages(): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        r.getCrawlDate != null
          && DetectMimeTypeTika(r.getBinaryBytes).startsWith("image/")
      )
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
    def keepDate(
        dates: List[String],
        component: DateComponent = DateComponent.YYYYMMDD
    ): RDD[ArchiveRecord] = {
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
        urlREs
          .map(re =>
            r.getUrl match {
              case re() => true
              case _    => false
            }
          )
          .exists(identity)
      )
    }

    /** Removes all data but selected source domains.
      *
      * @param urls a list of urls for the source domains
      */
    def keepDomains(urls: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        urls.contains(ExtractDomain(r.getUrl).replace("^\\s*www\\.", ""))
      )
    }

    /** Removes all data not in selected language.
      *
      * @param lang a set of ISO 639-2 codes
      */
    def keepLanguages(lang: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        lang.contains(DetectLanguage(RemoveHTML(r.getContentString)))
      )
    }

    /** Removes all content that does not pass Regular Expression test.
      *
      * @param contentREs a list of regular expressions to keep
      */
    def keepContent(contentREs: Set[Regex]): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        contentREs
          .map(re =>
            (re findFirstIn r.getContentString) match {
              case Some(v) => true
              case None    => false
            }
          )
          .exists(identity)
      )
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
    def discardDate(
        dates: List[String],
        component: DateComponent = DateComponent.YYYYMMDD
    ): RDD[ArchiveRecord] = {
      rdd.filter(r => !dates.contains(ExtractDate(r.getCrawlDate, component)))
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
        !urlREs
          .map(re =>
            r.getUrl match {
              case re() => true
              case _    => false
            }
          )
          .exists(identity)
      )
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
        !contentREs
          .map(re =>
            (re findFirstIn r.getContentString) match {
              case Some(v) => true
              case None    => false
            }
          )
          .exists(identity)
      )
    }

    /** Filters detected language.
      *
      * @param lang a set of ISO 639-2 codes
      */
    def discardLanguages(lang: Set[String]): RDD[ArchiveRecord] = {
      rdd.filter(r =>
        !lang.contains(DetectLanguage(RemoveHTML(r.getContentString)))
      )
    }
  }
}
