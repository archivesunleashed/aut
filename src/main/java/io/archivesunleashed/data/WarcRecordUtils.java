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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.httpclient.HttpParser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.log4j.Logger;
import org.archive.format.warc.WARCConstants;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;

/**
 * Utilities for working with {@code WARCRecord}s (from archive.org APIs).
 */
public final class WarcRecordUtils implements WARCConstants {

  /**
   * Utility classes should not have a public or default constructor.
   */
  private WarcRecordUtils() {
  }

  /**
  * Setup logger.
  */
  private static final Logger LOG = Logger.getLogger(WarcRecordUtils.class);

  /**
   * Converts raw bytes into an {@code WARCRecord}.
   *
   * @param bytes raw bytes
   * @return parsed {@code WARCRecord}
   * @throws IOException if there is an issue
   */
  public static WARCRecord fromBytes(final byte[] bytes) throws IOException {
    WARCReader reader = (WARCReader) WARCReaderFactory.get("",
        new BufferedInputStream(new ByteArrayInputStream(bytes)), false);
    return (WARCRecord) reader.get();
  }

  /**
   * Converts WARC record into raw bytes.
   *
   * @param record conents of WARC response record
   * @return raw contents
   * @throws IOException if there is an issue
   */
  public static byte[] toBytes(final WARCRecord record) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(baos);

    dout.write("WARC/0.17\n".getBytes());
    for (Map.Entry<String, Object> entry : record.getHeader()
            .getHeaderFields().entrySet()) {
      dout.write((entry.getKey() + ": " + entry.getValue().toString() + "\n")
              .getBytes());
    }
    dout.write("\n".getBytes());
    record.dump(dout);

    return baos.toByteArray();
  }

  /**
   * Extracts the MIME type of WARC response records.
   * "WARC-Type" is "response".
   * Note that this is different from the "Content-Type" in the WARC header.
   *
   * @param contents raw contents of the WARC response record
   * @return MIME type
   */
  public static String getWarcResponseMimeType(final byte[] contents) {
    // This is a somewhat janky way to get the MIME type of the response.
    // Moreover, this simple regex is not compliant with the specification.
    // See: https://www.w3.org/Protocols/rfc1341/4_Content-Type.html
    // It would be much better to parse all headers using an external library:
    //   org.apache.commons.httpclient.HeaderElement
    // Note that this is different from the "Content-Type" in the WARC header.
    Pattern pattern = Pattern.compile("Content-Type: ([^\\s;]+) *(;.*)?",
            Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(new String(contents));
    if (matcher.find()) {
      return matcher.group(1).replaceAll(";$", "");
    }

    return null;
  }

  /**
   * Extracts raw contents from a {@code WARCRecord} (including HTTP headers).
   *
   * @param record the {@code WARCRecord}
   * @return raw contents
   * @throws IOException if there is an issue
   */
  public static byte[] getContent(final WARCRecord record) throws IOException {
    int len = (int) record.getHeader().getContentLength();

    // If we have a corrupt record, quit and move on.
    if (len < 0) {
      return new byte[0];
    }

    try {
      return copyToByteArray(record, len, true);
    } catch (Exception e) {
      // Catch exceptions related to any corrupt archive files.
      return new byte[0];
    }
  }

  /**
   * Extracts contents of the body from a {@code WARCRecord}.
   * Excludes HTTP headers.
   *
   * @param record the {@code WARCRecord}
   * @return contents of the body
   * @throws IOException if there is an issue
   */
  public static byte[] getBodyContent(final WARCRecord record)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    String line = HttpParser.readLine(record, WARC_HEADER_ENCODING);
    if (line == null) {
      return null;
    }

    // Just using parseHeaders to move down input stream to body
    HttpParser.parseHeaders(record, WARC_HEADER_ENCODING);
    record.dump(baos);
    return baos.toByteArray();
  }

  /**
   * Copies contents to a byte array.
   *
   * @param is raw input stream
   * @param recordLength length of a record
   * @param enforceLength enforce the length
   * @return rawContents of body
   * @throws IOException if there is an issue
   */
  private static byte[] copyToByteArray(final InputStream is,
          final int recordLength, final boolean enforceLength)
      throws IOException {

    BoundedInputStream bis = new BoundedInputStream(is, recordLength);
    byte[] rawContents = IOUtils.toByteArray(bis);
    if (enforceLength && rawContents.length != recordLength) {
      LOG.error("Read " + rawContents.length + " bytes but expected "
              + recordLength + " bytes. Continuing...");
    }
    return rawContents;
  }
}
