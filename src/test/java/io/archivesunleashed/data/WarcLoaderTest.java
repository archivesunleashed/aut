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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.archive.util.ArchiveUtils;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;

public class WarcLoaderTest {
  private static final Log LOG = LogFactory.getLog(WarcLoaderTest.class);
  private static final SimpleDateFormat DATE_WARC =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

  @Test
  public final void testReader() throws Exception {
    String warcFile = Resources.getResource("warc/example.warc.gz").getPath();
    WARCReader reader = WARCReaderFactory.get(new File(warcFile));

    Object2IntFrequencyDistribution<String> types =
        new Object2IntFrequencyDistributionEntry<String>();

    Object2IntFrequencyDistribution<String> responseTypes =
        new Object2IntFrequencyDistributionEntry<String>();

    int cnt = 0;
    final int cntTest = 822;
    final int responseTest = 299;
    final int warcinfoTest = 1;
    final int requestTest = 261;
    final int metadataTest = 261;
    final int numberOfEventsTest = 4;
    final int typesSumOfCountsTest = 822;
    final int mimeTypeJavascriptTest = 8;
    final int mimeTypeCssTest = 4;
    final int mimeTypeFlashTest = 8;
    final int mimeTypeXmlTest = 9;
    final int mimeTypePngTest = 8;
    final int mimeTypeJpegTest = 18;
    final int mimeTypeGifTest = 29;
    final int mimeTypePlainTest = 36;
    final int mimeTypeHtmlTest = 140;
    final int responseSumOfCountsTest = 260;

    for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext();) {
      WARCRecord r = (WARCRecord) ii.next();
      ArchiveRecordHeader header = r.getHeader();

      types.increment((String) header.getHeaderValue("WARC-Type"));

      byte[] contents = WarcRecordUtils.getContent(r);
      int len = (int) header.getContentLength();
      assertEquals(len, contents.length);

      // This is how you extract the date
      @SuppressWarnings("unused")
      String digit14Date = ArchiveUtils.get14DigitDate(DATE_WARC.parse(header
                  .getDate()));

      if (header.getHeaderValue("WARC-Type").equals("response")
              && header.getUrl().startsWith("http://")) {
        responseTypes.increment(WarcRecordUtils.getWarcResponseMimeType(
                    contents));
      }

      cnt++;
    }
    reader.close();

    LOG.info(cnt + " records read!");
    assertEquals(cntTest, cnt);

    assertEquals(responseTest, types.get("response"));
    assertEquals(warcinfoTest, types.get("warcinfo"));
    assertEquals(requestTest, types.get("request"));
    assertEquals(metadataTest, types.get("metadata"));
    assertEquals(numberOfEventsTest, types.getNumberOfEvents());
    assertEquals(typesSumOfCountsTest, types.getSumOfCounts());

    assertEquals(mimeTypeJavascriptTest,
            responseTypes.get("application/x-javascript"));
    assertEquals(mimeTypeCssTest, responseTypes.get("text/css"));
    assertEquals(mimeTypeFlashTest,
            responseTypes.get("application/x-shockwave-flash"));
    assertEquals(mimeTypeXmlTest, responseTypes.get("text/xml"));
    assertEquals(mimeTypePngTest, responseTypes.get("image/png"));
    assertEquals(mimeTypeJpegTest, responseTypes.get("image/jpeg"));
    assertEquals(mimeTypeGifTest, responseTypes.get("image/gif"));
    assertEquals(mimeTypePlainTest, responseTypes.get("text/plain"));
    assertEquals(mimeTypeHtmlTest, responseTypes.get("text/html"));
    assertEquals(responseSumOfCountsTest, responseTypes.getSumOfCounts());
  }

  @Test
  public final void testReadFromStream() throws Exception {
    String warcFile = Resources.getResource("warc/example.warc.gz").getPath();
    WARCReader reader = WARCReaderFactory.get(new File(warcFile));

    int cnt = 0;
    final int cntTest = 822;

    for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext();) {
      WARCRecord r = (WARCRecord) ii.next();
      InputStream in = new DataInputStream(
          new ByteArrayInputStream(WarcRecordUtils.toBytes(r)));

      WARCReader nr = (WARCReader) WARCReaderFactory.get("",
          new BufferedInputStream(in), false);
      WARCRecord r2 = (WARCRecord) nr.get();

      assertEquals(r.getHeader().getUrl(), r2.getHeader().getUrl());

      ArchiveRecordHeader header = r2.getHeader();
      byte[] contents = WarcRecordUtils.getContent(r2);
      int len = (int) header.getContentLength();
      assertEquals(len, contents.length);

      cnt++;
    }
    reader.close();

    LOG.info(cnt + " records read!");
    assertEquals(cntTest, cnt);
  }

  @Test
  public final void testContentTypeWithCharset() throws Exception {
    String content = "Content-Type: text/html;charset=ISO-8859-1\r\n";
    byte[] contentBytes = content.getBytes("UTF-8");
    String mimeType = WarcRecordUtils.getWarcResponseMimeType(contentBytes);
    assertEquals("text/html", mimeType);
  }
}
