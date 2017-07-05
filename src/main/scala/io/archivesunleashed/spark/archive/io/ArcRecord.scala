package io.archivesunleashed.spark.archive.io

import org.apache.spark.SerializableWritable
import io.archivesunleashed.data.ArcRecordUtils
import io.archivesunleashed.io.ArcRecordWritable
import io.archivesunleashed.spark.matchbox.ExtractDate.DateComponent
import io.archivesunleashed.spark.matchbox.{RemoveHttpHeader, ExtractDate, ExtractDomain}

class ArcRecord(r: SerializableWritable[ArcRecordWritable]) extends ArchiveRecord {
  val getCrawlDate: String = ExtractDate(r.t.getRecord.getMetaData.getDate, DateComponent.YYYYMMDD)

  val getCrawlMonth: String = ExtractDate(r.t.getRecord.getMetaData.getDate, DateComponent.YYYYMM)

  val getMimeType: String = r.t.getRecord.getMetaData.getMimetype

  val getUrl: String = r.t.getRecord.getMetaData.getUrl

  val getDomain: String = ExtractDomain(r.t.getRecord.getMetaData.getUrl)

  val getContentBytes: Array[Byte] = ArcRecordUtils.getBodyContent(r.t.getRecord)

  val getContentString: String = new String(getContentBytes)

  val getImageBytes: Array[Byte] = {
    if (getContentString.startsWith("HTTP/"))
      getContentBytes.slice(
        getContentString.indexOf(RemoveHttpHeader.headerEnd)
          + RemoveHttpHeader.headerEnd.length, getContentBytes.length)
    else
      getContentBytes
  }
}
