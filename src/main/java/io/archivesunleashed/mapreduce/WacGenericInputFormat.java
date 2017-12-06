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
   *  Features here have been deprecated as of 0.11.0 and will be replaced
   *  with WacInputFormat in a future release.
   */

package io.archivesunleashed.mapreduce;

import io.archivesunleashed.io.GenericArchiveRecordWritable.ArchiveFormat;
import io.archivesunleashed.io.GenericArchiveRecordWritable;
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
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory.CompressedARCReader;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory.CompressedWARCReader;

/**
 * Extends FileInputFormat for Web Archive Commons Generic InputFormat.
 */
public class WacGenericInputFormat extends FileInputFormat<LongWritable,
       GenericArchiveRecordWritable> {
  @Override
  public final RecordReader<LongWritable,
  GenericArchiveRecordWritable> createRecordReader(final InputSplit split,
      final TaskAttemptContext context) throws IOException,
  InterruptedException {
    return new GenericArchiveRecordReader();
  }

  @Override
  protected final boolean isSplitable(final JobContext context,
          final Path filename) {
    return false;
  }

  /**
   * Extends RecordReader for Generic Record Reader.
   */
  public class GenericArchiveRecordReader extends RecordReader<LongWritable,
         GenericArchiveRecordWritable> {

    /**
     * Generic archive reader.
     */
    private ArchiveReader reader;

    /**
     * Generic archive format.
     */
    private ArchiveFormat format;

    /**
     * Start position of generic archive being read.
     */
    private long start;

    /**
     * A given position of a generic archive being read.
     */
    private long pos;

    /**
     * End position of a generic archive being read.
     */
    private long end;

    /**
     * LongWritable key.
     */
    private LongWritable key = null;

    /**
     * GenericArchiveRecordWritable value.
     */
    private GenericArchiveRecordWritable value = null;

    /**
     * Seekable file position.
     */
    private Seekable filePosition;

    /**
     * Iterator for generic archive record.
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
     * Determins if generic archive is compressed.
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
     * Get file position of generic archive.
     *
     * @return retVal position of generic archive
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
        value = new GenericArchiveRecordWritable();
      }
      value.setRecord(record);

      return true;
    }

    @Override
    public final LongWritable getCurrentKey() {
      return key;
    }

    @Override
    public final GenericArchiveRecordWritable getCurrentValue() {
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
