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

package io.archivesunleashed

import java.io.ByteArrayInputStream
import java.security.MessageDigest
import java.text.SimpleDateFormat

import io.archivesunleashed.data.{ArcRecordUtils, WarcRecordUtils, ArchiveRecordWritable}
import io.archivesunleashed.matchbox.{ComputeMD5RDD, ExtractDateRDD, ExtractDomainRDD, RemoveHTTPHeaderRDD}
import org.apache.commons.httpclient.{Header, HttpParser, StatusLine}
import org.apache.spark.SerializableWritable
import org.archive.io.arc.ARCRecord
import org.archive.io.warc.WARCRecord
import org.archive.util.ArchiveUtils
import scala.util.Try

/** Trait for a record in a web archive. */
trait ArchiveRecord extends Serializable {
  /** Returns the full path or url containing the Archive Records. */
  def getArchiveFilename: String

  /** Returns the crawl date. */
  def getCrawlDate: String

  /** Returns the crawl month. */
  def getCrawlMonth: String

  /** Returns the content of the record as an array of bytes. */
  def getContentBytes: Array[Byte]

  /** Returns the content of the record as a String. */
  def getContentString: String

  /** Returns the MIME type. */
  def getMimeType: String

  /** Returns the URL. */
  def getUrl: String

  /** Returns the domain. */
  def getDomain: String

  /** Returns a raw array of bytes for an image. */
  def getBinaryBytes: Array[Byte]

  /** Returns the http status of the crawl. */
  def getHttpStatus: String

  /** Returns payload digest (SHA1). */
  def getPayloadDigest: String
}

/** Default implementation of a record in a web archive.
 *
 *  @constructor an archive record.
 *  @param r the serialized record
 */
class ArchiveRecordImpl(r: SerializableWritable[ArchiveRecordWritable]) extends ArchiveRecord {
  val recordFormat = r.t.getFormat
  val ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

  val getArchiveFilename: String = {
    if (recordFormat == ArchiveRecordWritable.ArchiveFormat.ARC){
      r.t.getRecord.asInstanceOf[ARCRecord].getMetaData.getReaderIdentifier()
    } else {
      r.t.getRecord.asInstanceOf[WARCRecord].getHeader.getReaderIdentifier()
    }
  }

  val getCrawlDate: String = {
    if (recordFormat == ArchiveRecordWritable.ArchiveFormat.ARC){
      ExtractDateRDD(r.t.getRecord.asInstanceOf[ARCRecord].getMetaData.getDate,
        ExtractDateRDD.DateComponent.YYYYMMDD)
    } else {
      ExtractDateRDD(
        ArchiveUtils.get14DigitDate(
          ISO8601.parse(r.t.getRecord.asInstanceOf[WARCRecord].getHeader.getDate)),
        ExtractDateRDD.DateComponent.YYYYMMDD)
    }
  }

  val getCrawlMonth: String = {
    if (recordFormat == ArchiveRecordWritable.ArchiveFormat.ARC) {
      ExtractDateRDD(r.t.getRecord.asInstanceOf[ARCRecord].getMetaData.getDate,
        ExtractDateRDD.DateComponent.YYYYMM)
    } else {
      ExtractDateRDD(
        ArchiveUtils.get14DigitDate(
          ISO8601.parse(r.t.getRecord.asInstanceOf[WARCRecord].getHeader.getDate)),
        ExtractDateRDD.DateComponent.YYYYMM)
    }
  }

  val getContentBytes: Array[Byte] = {
    if (recordFormat == ArchiveRecordWritable.ArchiveFormat.ARC)
    {
      ArcRecordUtils.getContent(r.t.getRecord.asInstanceOf[ARCRecord])
    } else {
      WarcRecordUtils.getContent(r.t.getRecord.asInstanceOf[WARCRecord])
    }
  }

  val getContentString: String = {
    new String(getContentBytes)
  }

  val getMimeType: String = {
    if (recordFormat == ArchiveRecordWritable.ArchiveFormat.ARC) {
      Option(r.t.getRecord.asInstanceOf[ARCRecord].getMetaData.getMimetype).getOrElse("unknown")
    } else {
      Option(WarcRecordUtils.getWarcResponseMimeType(getContentBytes)).getOrElse("unknown")
    }
  }

  val getUrl: String = {
    if (r.t.getFormat == ArchiveRecordWritable.ArchiveFormat.ARC) {
      r.t.getRecord.asInstanceOf[ARCRecord].getMetaData.getUrl
    } else {
      r.t.getRecord.asInstanceOf[WARCRecord].getHeader.getUrl
    }
  }

  val getHttpStatus: String = {
    if (r.t.getFormat == ArchiveRecordWritable.ArchiveFormat.ARC) {
      Option(r.t.getRecord.asInstanceOf[ARCRecord].getMetaData.getStatusCode).getOrElse("000")
    } else {
      Try(new StatusLine(new String(HttpParser.readRawLine
        (new ByteArrayInputStream(getContentBytes))))
        .getStatusCode).toOption match {
          case Some(x) => x.toString
          case None => "000"
        }
    }
  }

  val getDomain: String = {
    ExtractDomainRDD(getUrl)
  }

  val getBinaryBytes: Array[Byte] = {
    if (getContentString.startsWith("HTTP/")) {
      getContentBytes.slice(
        getContentString.indexOf(RemoveHTTPHeaderRDD.headerEnd)
          + RemoveHTTPHeaderRDD.headerEnd.length, getContentBytes.length)
    } else {
      getContentBytes
    }
  }

  val getPayloadDigest: String = {
    if (recordFormat == ArchiveRecordWritable.ArchiveFormat.ARC){
      "sha1:" + MessageDigest.getInstance("SHA1").digest(getContentBytes).map("%02x".format(_)).mkString
    } else {
      r.t.getRecord.asInstanceOf[WARCRecord].getHeader.getHeaderValue("WARC-Payload-Digest").asInstanceOf[String]
    }
  }
}
