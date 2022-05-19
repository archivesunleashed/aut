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

import java.io.InputStream

import io.archivesunleashed.matchbox.ExtractDomain
import org.apache.tika.io.BoundedInputStream
import org.archive.webservices.sparkling.http.HttpMessage
import org.archive.webservices.sparkling.io.IOUtil
import org.archive.webservices.sparkling.util.{ManagedVal, ValueSupplier}
import org.archive.webservices.sparkling.warc.{WarcHeaders, WarcRecord}
import scala.util.Try

object SparklingArchiveRecord {
  val MaxStringByteLength: Int = 1024
}

class SparklingArchiveRecord(
    filename: String,
    meta: WarcRecord,
    payload: ManagedVal[ValueSupplier[InputStream]],
    maxMemoryBytes: Long = -1
) extends ArchiveRecord {
  import SparklingArchiveRecord._

  def warc: WarcRecord =
    new WarcRecord(meta.versionStr, meta.headers, payload.get.get)

  private def payload(r: WarcRecord): Array[Byte] = {
    IOUtil.bytes(
      if (maxMemoryBytes < 0) r.payload
      else new BoundedInputStream(maxMemoryBytes, r.payload)
    )
  }
  private def http(r: WarcRecord): Option[HttpMessage] = {
    if (maxMemoryBytes < 0) r.http
    else r.http.map(_.copy(maxBodyLength = maxMemoryBytes))
  }

  def limitBodyLength(maxBodyLength: Long): SparklingArchiveRecord = {
    new SparklingArchiveRecord(filename, meta, payload, maxBodyLength)
  }

  override def getArchiveFilename: String = filename
  override def getCrawlDate: String =
    meta.timestamp.filter(_.length >= 14).map(_.take(14)).getOrElse("")
  override def getCrawlMonth: String =
    warc.timestamp.filter(_.length >= 6).map(_.take(6)).getOrElse("")
  override def getContentBytes: Array[Byte] =
    Try {
      payload(warc)
    }.getOrElse(Array.empty)
  override def getContentString: String =
    Try {
      val record =
        if (maxMemoryBytes < 0) limitBodyLength(MaxStringByteLength).warc
        else warc
      http(record)
        .map { http =>
          new String(WarcHeaders.http(http.statusLine, http.headers)) + http.bodyString
        }
        .getOrElse(new String(payload(record)))
    }.getOrElse("")
  override def getMimeType: String =
    http(warc).flatMap(_.mime).getOrElse("unknown")
  override def getUrl: String = warc.url.getOrElse("").replaceAll("<|>", "")
  override def getDomain: String = ExtractDomain(getUrl)
  override def getBinaryBytes: Array[Byte] =
    Try {
      var record = warc
      http(record).map(_.body).map(IOUtil.bytes).getOrElse(payload(record))
    }.getOrElse(Array.empty)
  override def getHttpStatus: String =
    http(warc).map(_.status.toString).getOrElse("000")
  override def getPayloadDigest: String =
    Try {
      meta.payloadDigest.orElse(warc.digestPayload()).getOrElse("")
    }.getOrElse("")
}
