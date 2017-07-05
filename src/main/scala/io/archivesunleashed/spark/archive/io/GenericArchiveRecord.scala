package io.archivesunleashed.spark.archive.io

import java.text.SimpleDateFormat

import org.apache.spark.SerializableWritable
import org.archive.io.arc.ARCRecord
import org.archive.io.warc.WARCRecord

import org.archive.util.ArchiveUtils
import io.archivesunleashed.data.{ArcRecordUtils, WarcRecordUtils}
import io.archivesunleashed.io.GenericArchiveRecordWritable
import io.archivesunleashed.io.GenericArchiveRecordWritable.ArchiveFormat
import io.archivesunleashed.spark.matchbox.ExtractDate.DateComponent
import io.archivesunleashed.spark.matchbox.{RemoveHttpHeader, ExtractDate, ExtractDomain}

class GenericArchiveRecord(r: SerializableWritable[GenericArchiveRecordWritable]) extends ArchiveRecord {
  var arcRecord: ARCRecord = null
  var warcRecord: WARCRecord = null

  if (r.t.getFormat == ArchiveFormat.ARC)
    arcRecord = r.t.getRecord.asInstanceOf[ARCRecord]
  else if (r.t.getFormat == ArchiveFormat.WARC)
    warcRecord = r.t.getRecord.asInstanceOf[WARCRecord]


  val ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

  val getCrawlDate: String = {
    if (r.t.getFormat == ArchiveFormat.ARC) {
      ExtractDate(arcRecord.getMetaData.getDate, DateComponent.YYYYMMDD)
    } else {
      ExtractDate(ArchiveUtils.get14DigitDate(ISO8601.parse(warcRecord.getHeader.getDate)), DateComponent.YYYYMMDD)
    }
  }

  val getCrawlMonth: String = {
    if (r.t.getFormat == ArchiveFormat.ARC) {
      ExtractDate(arcRecord.getMetaData.getDate, DateComponent.YYYYMM)
    } else {
      ExtractDate(ArchiveUtils.get14DigitDate(ISO8601.parse(warcRecord.getHeader.getDate)), DateComponent.YYYYMM)
    }
  }

  val getContentBytes: Array[Byte] = {
    if (r.t.getFormat == ArchiveFormat.ARC) {
      ArcRecordUtils.getBodyContent(arcRecord)
    } else {
      WarcRecordUtils.getContent(warcRecord)
    }
  }

  val getContentString: String = new String(getContentBytes)

  val getMimeType = {
    if (r.t.getFormat == ArchiveFormat.ARC) {
      arcRecord.getMetaData.getMimetype
    } else {
      WarcRecordUtils.getWarcResponseMimeType(getContentBytes)
    }
  }

  val getUrl = {
    if (r.t.getFormat == ArchiveFormat.ARC) {
      arcRecord.getMetaData.getUrl
    } else {
      warcRecord.getHeader.getUrl
    }
  }

  val getDomain: String = ExtractDomain(getUrl)

  val getImageBytes: Array[Byte] = {
    if (getContentString.startsWith("HTTP/"))
      getContentBytes.slice(
        getContentString.indexOf(RemoveHttpHeader.headerEnd)
          + RemoveHttpHeader.headerEnd.length, getContentBytes.length)
    else
      getContentBytes
  }
}
