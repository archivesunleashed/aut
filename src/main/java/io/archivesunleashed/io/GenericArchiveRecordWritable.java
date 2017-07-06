/*
 * Warcbase: an open-source platform for managing web archives
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

package io.archivesunleashed.io;

import io.archivesunleashed.data.ArcRecordUtils;
import io.archivesunleashed.data.WarcRecordUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;

/**
 * Implements Hadoop Writable for Generic Archive Records.
 */
public class GenericArchiveRecordWritable implements Writable {

  /**
   * Archive Formats that can be used.
   * <li>{@link #UNKNOWN}</li>
   * <li>{@link #ARC}</li>
   * <li>{@link #WARC}</li>
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
   * Set default Generic Record format to UNKNOWN.
   */
  private ArchiveFormat format = ArchiveFormat.UNKNOWN;

  /**
   * Initialize Generic Archive Record to null.
   */
  private ArchiveRecord record = null;

  /**
   * Utility function.
   */
  public GenericArchiveRecordWritable() {
  }

  /**
   * Initialize Generic Archive Record.
   *
   * @param r Generic Archive Record
   */
  public GenericArchiveRecordWritable(final ArchiveRecord r) {
    this.record = r;
    detectFormat();
  }

  /**
   * Set Generic Archive Record.
   *
   * @param r Generic Archive Record
   */
  public final void setRecord(final ArchiveRecord r) {
    this.record = r;
    detectFormat();
  }

  /**
   * Get Generic Archive Record.
   *
   * @return record Generic Archive Record
   */
  public final ArchiveRecord getRecord() {
    return record;
  }

  /**
   * Detect format of Generic Archive Record.
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
   * Get format of Generic Archive Record.
   *
   * @return format of Generic Archive Record
   */
  public final ArchiveFormat getFormat() {
    return format;
  }

  /**
   * Set format of Generic Archive Record.
   *
   * @param f format of Generic Archive Record
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
