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

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;
import org.archive.io.warc.WARCRecord;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArchiveRecordInputFormatTest {
  @Test
  public final void testArcInputFormat() throws Exception {
    String[] urls = new String[]{
            "filedesc://IAH-20080430204825-00000-blackbook.arc",
            "dns:www.archive.org",
            "http://www.archive.org/robots.txt",
            "http://www.archive.org/",
            "http://www.archive.org/index.php"};

    String arcFile = Resources.getResource("arc/example.arc.gz").getPath();

    Configuration conf = new Configuration(false);
    conf.set("fs.defaultFS", "file:///");

    File testFile = new File(arcFile);
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat<LongWritable, ArchiveRecordWritable> inputFormat =
            ReflectionUtils.newInstance(ArchiveRecordInputFormat.class, conf);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf,
            new TaskAttemptID());
    RecordReader<LongWritable, ArchiveRecordWritable> reader =
            inputFormat.createRecordReader(split, context);

    reader.initialize(split, context);

    int cnt = 0;
    final int cntTest = 300;

    while (reader.nextKeyValue()) {
      ArchiveRecord record = reader.getCurrentValue().getRecord();
      boolean isArc = record instanceof ARCRecord;
      assertTrue(isArc);

      if (isArc) {
        ARCRecord arcRecord = (ARCRecord) record;
        ARCRecordMetaData metadata = arcRecord.getMetaData();

        if (cnt < urls.length) {
          assertEquals(urls[cnt], metadata.getUrl());
        }
      }

      cnt++;
    }
    assertEquals(cntTest, cnt);
  }

  @Test
  public final void testWarcInputFormat() throws Exception {
    String[] urls = new String[]{
        null,
        "dns:www.archive.org",
        "http://www.archive.org/robots.txt",
        "http://www.archive.org/robots.txt",
        "http://www.archive.org/robots.txt",
        "http://www.archive.org/",
        "http://www.archive.org/",
        "http://www.archive.org/",
        "http://www.archive.org/index.php",
        "http://www.archive.org/index.php"};

    String[] types = new String[]{
        "warcinfo",
        "response",
        "response",
        "request",
        "metadata",
        "response",
        "request",
        "metadata",
        "response",
        "request"};

    String arcFile = Resources.getResource("warc/example.warc.gz").getPath();

    Configuration conf = new Configuration(false);
    conf.set("fs.defaultFS", "file:///");

    File testFile = new File(arcFile);
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat<LongWritable, ArchiveRecordWritable> inputFormat =
            ReflectionUtils.newInstance(ArchiveRecordInputFormat.class, conf);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf,
            new TaskAttemptID());
    RecordReader<LongWritable, ArchiveRecordWritable> reader =
            inputFormat.createRecordReader(split, context);

    reader.initialize(split, context);

    assertTrue(urls.length == types.length);

    int cnt = 0;
    int responseCnt = 0;
    final int cntTest = 822;
    final int responseCntTest = 299;

    while (reader.nextKeyValue()) {
      ArchiveRecord record = reader.getCurrentValue().getRecord();
      boolean isWarc = record instanceof WARCRecord;
      assertTrue(isWarc);

      if (isWarc) {
        WARCRecord warcRecord = (WARCRecord) record;
        if (cnt < urls.length) {
          assertEquals(urls[cnt], warcRecord.getHeader().getUrl());
          assertEquals(types[cnt], warcRecord.getHeader()
                  .getHeaderValue("WARC-Type"));
        }

        if (warcRecord.getHeader().getHeaderValue("WARC-Type")
                .equals("response")) {
          responseCnt++;
        }
      }

      cnt++;
    }
    assertEquals(cntTest, cnt);
    assertEquals(responseCntTest, responseCnt);
  }
}
