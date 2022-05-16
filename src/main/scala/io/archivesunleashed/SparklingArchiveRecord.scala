package io.archivesunleashed

import java.io.InputStream

import io.archivesunleashed.matchbox.ExtractDomain
import org.apache.tika.io.BoundedInputStream
import org.archive.webservices.sparkling.http.HttpMessage
import org.archive.webservices.sparkling.io.IOUtil
import org.archive.webservices.sparkling.util.{ManagedVal, ValueSupplier}
import org.archive.webservices.sparkling.warc.{WarcHeaders, WarcRecord}

object SparklingArchiveRecord {
  val MaxStringByteLength: Int = 1024
}

class SparklingArchiveRecord(filename: String, meta: WarcRecord, payload: ManagedVal[ValueSupplier[InputStream]], maxMemoryBytes: Long = -1) extends ArchiveRecord {
  import SparklingArchiveRecord._

  def warc: WarcRecord = new WarcRecord(meta.versionStr, meta.headers, payload.get.get)

  private def payload(r: WarcRecord): Array[Byte] = {
    IOUtil.bytes(if (maxMemoryBytes < 0) r.payload else new BoundedInputStream(maxMemoryBytes, r.payload))
  }
  private def http(r: WarcRecord): Option[HttpMessage] = {
    if (maxMemoryBytes < 0) r.http else r.http.map(_.copy(maxBodyLength = maxMemoryBytes))
  }

  def limitBodyLength(maxBodyLength: Long): SparklingArchiveRecord = {
    new SparklingArchiveRecord(filename, meta, payload, maxBodyLength)
  }

  override def getArchiveFilename: String = filename
  override def getCrawlDate: String = meta.timestamp.filter(_.length >= 8).map(_.take(8)).getOrElse("")
  override def getCrawlMonth: String = warc.timestamp.filter(_.length >= 6).map(_.take(6)).getOrElse("")
  override def getContentBytes: Array[Byte] = payload(warc)
  override def getContentString: String = {
    val record = if (maxMemoryBytes < 0) limitBodyLength(MaxStringByteLength).warc else warc
    http(record).map { http =>
      new String(WarcHeaders.http(http.statusLine, http.headers)) + http.bodyString
    }.getOrElse(new String(payload(record)))
  }
  override def getMimeType: String = http(warc).flatMap(_.mime).getOrElse("unknown")
  override def getUrl: String = warc.url.getOrElse("")
  override def getDomain: String = ExtractDomain(getUrl)
  override def getBinaryBytes: Array[Byte] = {
    var record = warc
    http(record).map(_.body).map(IOUtil.bytes).getOrElse(payload(record))
  }
  override def getHttpStatus: String = http(warc).map(_.status.toString).getOrElse("000")
  override def getPayloadDigest: String = meta.payloadDigest.orElse(warc.digestPayload()).getOrElse("")
}
