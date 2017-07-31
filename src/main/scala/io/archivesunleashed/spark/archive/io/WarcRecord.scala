/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source platform for analyzing web archives.
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
package io.archivesunleashed.spark.archive.io

import java.text.SimpleDateFormat

import org.apache.spark.SerializableWritable
import org.archive.util.ArchiveUtils
import io.archivesunleashed.data.WarcRecordUtils
import io.archivesunleashed.io.WarcRecordWritable
import io.archivesunleashed.spark.matchbox.ExtractDate.DateComponent
import io.archivesunleashed.spark.matchbox.{RemoveHttpHeader, ExtractDate, ExtractDomain}

class WarcRecord(r: SerializableWritable[WarcRecordWritable]) extends ArchiveRecord {
  val ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

  val getCrawlDate: String = ExtractDate(ArchiveUtils.get14DigitDate(ISO8601.parse(r.t.getRecord.getHeader.getDate)), DateComponent.YYYYMMDD)

  val getCrawlMonth: String = ExtractDate(ArchiveUtils.get14DigitDate(ISO8601.parse(r.t.getRecord.getHeader.getDate)), DateComponent.YYYYMM)

  val getContentBytes: Array[Byte] = WarcRecordUtils.getContent(r.t.getRecord)

  val getContentString: String = new String(getContentBytes)

  val getMimeType = WarcRecordUtils.getWarcResponseMimeType(getContentBytes)

  val getUrl = r.t.getRecord.getHeader.getUrl

  val getDomain = ExtractDomain(getUrl)

  val getImageBytes: Array[Byte] = {
    if (getContentString.startsWith("HTTP/"))
      getContentBytes.slice(
        getContentString.indexOf(RemoveHttpHeader.headerEnd)
          + RemoveHttpHeader.headerEnd.length, getContentBytes.length)
    else
      getContentBytes
  }
}
