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

 /**
   *  @deprecated as of 0.11.0 and will be removed
   *  in a future release. Use GenericArchiveRecordWritable (0.11.0) or
   *  ArchiveRecordWritable (future releases) instead.
   */

package io.archivesunleashed.io;

import io.archivesunleashed.data.ArcRecordUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.archive.io.arc.ARCRecord;

/**
 * Implements Hadoop Writable for ARC Records.
 */
@Deprecated
public class ArcRecordWritable implements Writable {

  /**
   * Initialize ARC Record to null.
   */
  private ARCRecord record = null;

  /**
   * Utility function.
   */
  public ArcRecordWritable() {
  }

  /**
   * Initialize ARC Record.
   *
   * @param r ARC Record
   */
  public ArcRecordWritable(final ARCRecord r) {
    this.record = r;
  }

  /**
   * Set ARC Record.
   *
   * @param r ARC Record
   */
  public final void setRecord(final ARCRecord r) {
    this.record = r;
  }

  /**
   * Get ARC Record.
   *
   * @return record ARC Record
   */
  public final ARCRecord getRecord() {
    return record;
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

    this.record = ArcRecordUtils.fromBytes(bytes);
  }

  @Override
  public final void write(final DataOutput out) throws IOException {
    if (record == null) {
      out.writeInt(0);
    }
    byte[] bytes = ArcRecordUtils.toBytes(record);

    out.writeInt(bytes.length);
    out.write(bytes);
  }
}
