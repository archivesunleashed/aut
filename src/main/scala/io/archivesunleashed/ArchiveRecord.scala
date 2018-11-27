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

package io.archivesunleashed

import java.text.SimpleDateFormat
import java.io.ByteArrayInputStream

import io.archivesunleashed.data.{ArcRecordUtils, WarcRecordUtils, ArchiveRecordWritable}
import io.archivesunleashed.matchbox.{ExtractDate, ExtractDomain, RemoveHttpHeader}
import org.apache.spark.SerializableWritable
import org.archive.io.arc.ARCRecord
import org.archive.io.warc.WARCRecord
import org.archive.util.ArchiveUtils
import scala.util.Try
import org.apache.commons.httpclient.{Header, HttpParser, StatusLine}

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
  def getImageBytes: Array[Byte]

  /** Returns the http status of the crawl. */
  def getHttpStatus: String

}

/** Default implementation of a record in a web archive.
 *
 *  @constructor an archive record.
 *  @param r the serialized record
 */
class ArchiveRecordImpl(r: SerializableWritable[ArchiveRecordWritable]) extends ArchiveRecord {
  // Option<t> would require refactor of methods. Ignore.
  // scalastyle:off null
  var arcRecord: ARCRecord = null
  var warcRecord: WARCRecord = null
  // scalastyle:on null
  var headerResponseFormat: String = "US-ASCII"

  if (r.t.getFormat == ArchiveRecordWritable.ArchiveFormat.ARC) {
    arcRecord = r.t.getRecord.asInstanceOf[ARCRecord]
  } else if (r.t.getFormat == ArchiveRecordWritable.ArchiveFormat.WARC) {
      warcRecord = r.t.getRecord.asInstanceOf[WARCRecord]
  }
  val ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

  val getArchiveFilename: String = {
    if (r.t.getFormat == ArchiveRecordWritable.ArchiveFormat.ARC){
      arcRecord.getMetaData.getReaderIdentifier()
    } else {
      warcRecord.getHeader.getReaderIdentifier()
    }
  }

  val getCrawlDate: String = {
    if (r.t.getFormat == ArchiveRecordWritable.ArchiveFormat.ARC){
      ExtractDate(arcRecord.getMetaData.getDate, ExtractDate.DateComponent.YYYYMMDD)
    } else {
      ExtractDate(
        ArchiveUtils.get14DigitDate(
          ISO8601.parse(warcRecord.getHeader.getDate)), ExtractDate.DateComponent.YYYYMMDD)
    }
  }

  val getCrawlMonth: String = {
    if (r.t.getFormat == ArchiveRecordWritable.ArchiveFormat.ARC) {
      ExtractDate(arcRecord.getMetaData.getDate, ExtractDate.DateComponent.YYYYMM)
    } else {
      ExtractDate(
        ArchiveUtils.get14DigitDate(
          ISO8601.parse(warcRecord.getHeader.getDate)), ExtractDate.DateComponent.YYYYMM)
    }
  }

  val getContentBytes: Array[Byte] = {
    if (r.t.getFormat == ArchiveRecordWritable.ArchiveFormat.ARC)
    {
      ArcRecordUtils.getBodyContent(arcRecord)
    } else {
      WarcRecordUtils.getContent(warcRecord)
    }
  }

  val getContentString: String = {
    new String(getContentBytes)
  }

  val getMimeType: String = {
    if (r.t.getFormat == ArchiveRecordWritable.ArchiveFormat.ARC) {
      Option(arcRecord.getMetaData.getMimetype).getOrElse("unknown")
    } else {
      Option(WarcRecordUtils.getWarcResponseMimeType(getContentBytes))
        .getOrElse("unknown")
    }
  }

  val getUrl: String = {
    if (r.t.getFormat == ArchiveRecordWritable.ArchiveFormat.ARC) {
      arcRecord.getMetaData.getUrl
    } else {
      warcRecord.getHeader.getUrl
    }
  }

  val getHttpStatus: String = {
    if (r.t.getFormat == ArchiveRecordWritable.ArchiveFormat.ARC) {
      Option(arcRecord.getMetaData.getStatusCode).getOrElse("000")
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
    ExtractDomain(getUrl)
  }

  val getImageBytes: Array[Byte] = {
    if (getContentString.startsWith("HTTP/")) {
      getContentBytes.slice(
        getContentString.indexOf(RemoveHttpHeader.headerEnd)
          + RemoveHttpHeader.headerEnd.length, getContentBytes.length)
    } else {
      getContentBytes
    }
  }
}
