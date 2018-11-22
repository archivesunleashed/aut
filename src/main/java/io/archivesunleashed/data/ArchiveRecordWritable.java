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

package io.archivesunleashed.data;

import org.apache.hadoop.io.Writable;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implements Hadoop Writable for Archive Records.
 */
public class ArchiveRecordWritable implements Writable {

  /**
   * Archive Formats that can be used.
   * {@link #UNKNOWN}
   * {@link #ARC}
   * {@link #WARC}
   */
  public enum ArchiveFormat {
      /**
       * UNKNOWN format.
       */
      UNKNOWN,

      /**
       * ARC format.
       */
      ARC,

      /**
       * WARC format.
       */
      WARC
  }

  /**
   * Set default Record format to UNKNOWN.
   */
  private ArchiveFormat format = ArchiveFormat.UNKNOWN;

  /**
   * Initialize Archive Record to null.
   */
  private ArchiveRecord record = null;

  /**
   * Utility function.
   */
  public ArchiveRecordWritable() {
  }

  /**
   * Initialize Archive Record.
   *
   * @param r Archive Record
   */
  public ArchiveRecordWritable(final ArchiveRecord r) {
    this.record = r;
    detectFormat();
  }

  /**
   * Set Archive Record.
   *
   * @param r Archive Record
   */
  public final void setRecord(final ArchiveRecord r) {
    this.record = r;
    detectFormat();
  }

  /**
   * Get Archive Record.
   *
   * @return record Archive Record
   */
  public final ArchiveRecord getRecord() {
    return record;
  }

  /**
   * Detect format of Archive Record.
   */
  public final void detectFormat() {
    if (record instanceof ARCRecord) {
      format = ArchiveFormat.ARC;
    } else if (record instanceof WARCRecord)  {
      format = ArchiveFormat.WARC;
    } else {
      format = ArchiveFormat.UNKNOWN;
    }
  }

  /**
   * Get format of Archive Record.
   *
   * @return format of Archive Record
   */
  public final ArchiveFormat getFormat() {
    return format;
  }

  /**
   * Set format of Archive Record.
   *
   * @param f format of Archive Record
   */
  public final void setFormat(final ArchiveFormat f) {
    this.format = f;
  }

  @Override
  public final void readFields(final DataInput in) throws IOException {
    int len = in.readInt();
    if (len == 0) {
      this.record = null;
      return;
    }

    byte[] bytes = new byte[len];
    in.readFully(bytes);

    if (getFormat() == ArchiveFormat.ARC) {
      this.record = ArcRecordUtils.fromBytes(bytes);
    } else if (getFormat() == ArchiveFormat.WARC) {
      this.record = WarcRecordUtils.fromBytes(bytes);
    } else {
      this.record = null;
    }
  }

  @Override
  public final void write(final DataOutput out) throws IOException {
    if (record == null) {
      out.writeInt(0);
    }
    byte[] bytes;

    if (getFormat() == ArchiveFormat.ARC) {
      bytes = ArcRecordUtils.toBytes((ARCRecord) record);
    } else if (getFormat() == ArchiveFormat.WARC) {
      bytes = WarcRecordUtils.toBytes((WARCRecord) record);
    } else {
      bytes = null;
    }

    out.writeInt(bytes.length);
    out.write(bytes);
  }
}
