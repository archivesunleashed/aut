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
import org.slf4j.LoggerFactory
import scala.util.Try

/** Set MaxStringByteLength and MaxBinaryBytes for SparklingArchiveRecord. */
object SparklingArchiveRecord {
  val MaxStringByteLength: Int = 1024

  /**
   * Largest payload, in bytes, that getBinaryBytes will load into memory. Larger records are
   * skipped (an empty array is returned) instead of being read.
   *
   * A JVM byte array cannot hold much more than 2 GiB, so reading a bigger payload throws an
   * OutOfMemoryError. That is a fatal error, which the Try in getBinaryBytes does not catch, so
   * a single oversized record would otherwise fail its task, and after Spark's retries the whole
   * job, no matter how much heap is available. Override with -Daut.maxBinaryBytes=<bytes>.
   */
  val MaxBinaryBytes: Long = sys.props
    .get("aut.maxBinaryBytes")
    .flatMap(s => Try(s.toLong).toOption)
    .filter(_ > 0)
    .getOrElse(512L * 1024 * 1024)

  private val log = LoggerFactory.getLogger(classOf[SparklingArchiveRecord])

  /**
   * Reads a stream fully, without ever holding more than max + 1 bytes.
   *
   * @return
   *   the bytes, or None if the stream holds more than max bytes.
   */
  private[archivesunleashed] def readBounded(in: InputStream, max: Long): Option[Array[Byte]] = {
    val bytes = IOUtil.bytes(new BoundedInputStream(max + 1, in))
    if (bytes.length > max) None else Some(bytes)
  }
}

/**
 * Extension of ArchiveRecord via Sparkling.
 */
class SparklingArchiveRecord(
    filename: String,
    meta: WarcRecord,
    payload: ManagedVal[ValueSupplier[InputStream]],
    maxMemoryBytes: Long = -1)
    extends ArchiveRecord {
  import SparklingArchiveRecord._

  def warc: WarcRecord =
    new WarcRecord(meta.versionStr, meta.headers, payload.get.get)

  private def payloadStream(r: WarcRecord): InputStream =
    if (maxMemoryBytes < 0) r.payload
    else new BoundedInputStream(maxMemoryBytes, r.payload)

  private def payload(r: WarcRecord): Array[Byte] = IOUtil.bytes(payloadStream(r))
  private def http(r: WarcRecord): Option[HttpMessage] = {
    if (maxMemoryBytes < 0) r.http
    else r.http.map(_.copy(maxBodyLength = maxMemoryBytes))
  }

  def limitBodyLength(maxBodyLength: Long): SparklingArchiveRecord = {
    new SparklingArchiveRecord(filename, meta, payload, maxBodyLength)
  }

  override def getLastModified: String =
    http(warc)
      .flatMap(_.headerMap.get("last-modified"))
      .getOrElse("")

  override def getArchiveFilename: String = filename

  override def getCrawlDate: String =
    meta.timestamp.filter(_.length >= 14).map(_.take(14)).getOrElse("")

  override def getCrawlMonth: String =
    warc.timestamp.filter(_.length >= 6).map(_.take(6)).getOrElse("")

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
    http(warc)
      .flatMap(_.mime)
      .getOrElse("unknown")
      .replaceAll(" .*|\\s|\\n", "")

  override def getUrl: String = warc.url.getOrElse("").replaceAll("<|>", "")

  override def getDomain: String = ExtractDomain(getUrl)

  override def getBinaryBytes: Array[Byte] =
    Try {
      val record = warc
      val stream = http(record).map(_.body).getOrElse(payloadStream(record))
      readBounded(stream, MaxBinaryBytes).getOrElse {
        log.warn(
          s"Skipping payload over $MaxBinaryBytes bytes: ${meta.url.getOrElse("")} in $filename")
        Array.empty[Byte]
      }
    }.getOrElse(Array.empty)

  override def getHttpStatus: String =
    http(warc).map(_.status.toString).getOrElse("000")

  override def getPayloadDigest: String =
    Try {
      meta.payloadDigest.getOrElse(warc.digestPayload())
    }.getOrElse("")
}
