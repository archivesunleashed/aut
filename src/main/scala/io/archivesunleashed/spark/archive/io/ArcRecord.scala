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

import org.apache.spark.SerializableWritable
import io.archivesunleashed.data.ArcRecordUtils
import io.archivesunleashed.io.ArcRecordWritable
import io.archivesunleashed.spark.matchbox.ExtractDate.DateComponent
import io.archivesunleashed.spark.matchbox.{RemoveHttpHeader, ExtractDate, ExtractDomain}

class ArcRecord(r: SerializableWritable[ArcRecordWritable]) extends ArchiveRecord {
  val getCrawlDate: String = ExtractDate(r.t.getRecord.getMetaData.getDate, DateComponent.YYYYMMDD)

  val getCrawlMonth: String = ExtractDate(r.t.getRecord.getMetaData.getDate, DateComponent.YYYYMM)

  val getCrawlYear: String = ExtractDate(r.t.getRecord.getMetaData.getDate, DateComponent.YYYY)

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
