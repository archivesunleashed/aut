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

import io.archivesunleashed.data.ArchiveRecordWritable.ArchiveFormat;
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
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory.CompressedARCReader;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory.CompressedWARCReader;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * Extends FileInputFormat for Web Archive Commons InputFormat.
 */
public class ArchiveRecordInputFormat extends FileInputFormat<LongWritable,
  ArchiveRecordWritable> {
  @Override
  public final RecordReader<LongWritable,
    ArchiveRecordWritable> createRecordReader(final InputSplit split,
      final TaskAttemptContext context) throws IOException,
  InterruptedException {
    return new ArchiveRecordReader();
  }

  @Override
  protected final boolean isSplitable(final JobContext context,
          final Path filename) {
    return false;
  }

  /**
   * Extends RecordReader for Record Reader.
   */
  public class ArchiveRecordReader extends RecordReader<LongWritable,
    ArchiveRecordWritable> {

    /**
     * Archive reader.
     */
    private ArchiveReader reader;

    /**
     * Archive format.
     */
    private ArchiveFormat format;

    /**
     * Start position of archive being read.
     */
    private long start;

    /**
     * A given position of an archive being read.
     */
    private long pos;

    /**
     * End position of an archive being read.
     */
    private long end;

    /**
     * LongWritable key.
     */
    private LongWritable key = null;

    /**
     * ArchiveRecordWritable value.
     */
    private ArchiveRecordWritable value = null;

    /**
     * Seekable file position.
     */
    private Seekable filePosition;

    /**
     * Iterator for an archive record.
     */
    private Iterator<ArchiveRecord> iter;

    @Override
    public final void initialize(final InputSplit archiveRecordSplit,
            final TaskAttemptContext context)
    throws IOException {
      FileSplit split = (FileSplit) archiveRecordSplit;
      Configuration job = context.getConfiguration();
      start = split.getStart();
      end = start + split.getLength();
      final Path file = split.getPath();

      FileSystem fs = file.getFileSystem(job);
      FSDataInputStream fileIn = fs.open(split.getPath());

      reader = ArchiveReaderFactory.get(split.getPath().toString(),
          new BufferedInputStream(fileIn), true);

      if (reader instanceof ARCReader) {
        format = ArchiveFormat.ARC;
        iter = reader.iterator();
      }

      if (reader instanceof WARCReader) {
        format = ArchiveFormat.WARC;
        iter = reader.iterator();
      }

      this.pos = start;
    }

    /**
     * Determines if archive is compressed.
     *
     * @return instanceof if ARC/WARC
     */
    private boolean isCompressedInput() {
      if (format == ArchiveFormat.ARC) {
        return reader instanceof CompressedARCReader;
      } else {
        return reader instanceof CompressedWARCReader;
      }
    }

    /**
     * Get file position of archive.
     *
     * @return retVal position of archive
     * @throws IOException if there is an issue
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

      ArchiveRecord record = null;
      try {
        record = iter.next();
      } catch (Exception e) {
        return false;
      }

      if (record == null) {
        return false;
      }

      if (value == null) {
        value = new ArchiveRecordWritable();
      }
      value.setRecord(record);

      return true;
    }

    @Override
    public final LongWritable getCurrentKey() {
      return key;
    }

    @Override
    public final ArchiveRecordWritable getCurrentValue() {
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
