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

package io.archivesunleashed.mapreduce;

import io.archivesunleashed.io.WarcRecordWritable;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory.CompressedWARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;

/**
 * Extends FileInputFormat for Web Archive Commons WARC InputFormat.
 */
public class WacWarcInputFormat extends FileInputFormat<LongWritable,
       WarcRecordWritable> {
  @Override
  public final RecordReader<LongWritable, WarcRecordWritable>
  createRecordReader(
          final InputSplit split,
      final TaskAttemptContext context) throws IOException,
  InterruptedException {
    return new WarcRecordReader();
  }

  @Override
  protected final boolean isSplitable(final JobContext context,
          final Path filename) {
    return false;
  }

  /**
   * Extends RecordReader for WARC Record Reader.
   */
  public class WarcRecordReader extends RecordReader<LongWritable,
         WarcRecordWritable> {

    /**
     * WARC reader.
     */
    private WARCReader reader;

    /**
     * Start position of WARC being read.
     */
    private long start;

    /**
     * A given position of a WARC being read.
     */
    private long pos;

    /**
     * End position of a WARC being read.
     */
    private long end;

    /**
     * LongWritable key.
     */
    private LongWritable key = null;

    /**
     * WarcRecordWritable value.
     */
    private WarcRecordWritable value = null;

    /**
     * Seekable file position.
     */
    private Seekable filePosition;

    /**
     * Iterator for ArchiveRecord.
     */
    private Iterator<ArchiveRecord> iter;

    @Override
    public final void initialize(final InputSplit genericSplit,
            final TaskAttemptContext context)
    throws IOException {
      FileSplit split = (FileSplit) genericSplit;
      Configuration job = context.getConfiguration();
      start = split.getStart();
      end = start + split.getLength();
      final Path file = split.getPath();

      FileSystem fs = file.getFileSystem(job);
      FSDataInputStream fileIn = fs.open(split.getPath());

      reader = (WARCReader) WARCReaderFactory.get(split.getPath().toString(),
          new BufferedInputStream(fileIn), true);

      iter = reader.iterator();
      //reader = (ARCReader) ARCReaderFactory.get(split.getPath().toString(),
      //fileIn, true);

      this.pos = start;
    }

    /**
     * Determins if WARC is compressed.
     *
     * @return reader true/false
     */
    private boolean isCompressedInput() {
      return reader instanceof CompressedWARCReader;
    }

    /**
     * Get file position of WARC.
     *
     * @return retVal position of WARC
     * @throws IOException is there is an issue
     */
    private long getFilePosition() throws IOException {
      long retVal;
      if (isCompressedInput() && null != filePosition) {
        retVal = filePosition.getPos();
      } else {
        retVal = pos;
      }
      return retVal;
    }

    @Override
    public final boolean nextKeyValue() throws IOException {
      if (!iter.hasNext()) {
        return false;
      }

      if (key == null) {
        key = new LongWritable();
      }
      key.set(pos);

      WARCRecord record = (WARCRecord) iter.next();
      if (record == null) {
        return false;
      }

      if (value == null) {
        value = new WarcRecordWritable();
      }
      value.setRecord(record);

      return true;
    }

    @Override
    public final LongWritable getCurrentKey() {
      return key;
    }

    @Override
    public final WarcRecordWritable getCurrentValue() {
      return value;
    }

    @Override
    public final float getProgress() throws IOException {
      if (start == end) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (getFilePosition() - start) / (float)
                (end - start));
      }
    }

    @Override
    public final synchronized void close() throws IOException {
      reader.close();
    }
  }
}
