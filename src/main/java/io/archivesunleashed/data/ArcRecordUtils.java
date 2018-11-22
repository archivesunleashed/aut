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
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.log4j.Logger;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;

/**
 * Utilities for working with {@code ARCRecord}s (from archive.org APIs).
 */
public final class ArcRecordUtils {

  /**
   * Utility classes should not have a public or default constructor.
   */
  private ArcRecordUtils() {
  }

  /**
   * Setup logger.
   */
  private static final Logger LOG = Logger.getLogger(ArcRecordUtils.class);

  /**
   * Converts raw bytes into an {@code ARCRecord}.
   *
   * @param bytes raw bytes
   * @return parsed {@code ARCRecord}
   * @throws IOException if there is an issue
   */
  public static ARCRecord fromBytes(final byte[] bytes) throws IOException {
    ARCReader reader = (ARCReader) ARCReaderFactory.get("",
        new BufferedInputStream(new ByteArrayInputStream(bytes)), false);
    return (ARCRecord) reader.get();
  }

  /**
   * Converts ARC record into raw bytes.
   *
   * @param record conents of WARC response record
   * @return raw contents
   * @throws IOException if there is an issue
   */
  public static byte[] toBytes(final ARCRecord record) throws IOException {
    ARCRecordMetaData meta = record.getMetaData();

    String metaline = meta.getUrl() + " " + meta.getIp()
            + " " + meta.getDate() + " " + meta.getMimetype()
            + " " + (int) meta.getLength();
    String versionEtc = "";


    if (meta.getOffset() == 0) {
      versionEtc = "\n" + meta.getVersion().replace(".", " ")
              + " " + meta.getOrigin() + "\n"
              + "URL IP-address Archive-date Content-type Archive-length";
      metaline += versionEtc;
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(baos);
    dout.write(metaline.getBytes());
    dout.write("\n".getBytes());

    long recordLength = meta.getLength() - versionEtc.length();
    long len = IOUtils.copyLarge(new BoundedInputStream(record, recordLength),
            dout);
    if (len != recordLength) {
      LOG.error("Read " + len + " bytes but expected " + recordLength
              + " bytes. Continuing...");
    }
    return baos.toByteArray();
  }

  /**
   * Extracts raw contents from an {@code ARCRecord} (including HTTP headers).
   *
   * @param record the {@code ARCRecord}
   * @return raw contents
   * @throws IOException if there is an issue
   */
  public static byte[] getContent(final ARCRecord record) throws IOException {
    ARCRecordMetaData meta = record.getMetaData();
    String versionEtc = "";

    if (meta.getOffset() == 0) {
      versionEtc = "\n" + meta.getVersion().replace(".", " ")
              + " " + meta.getOrigin() + "\n"
              + "URL IP-address Archive-date Content-type Archive-length";
    }

    try {
      return copyToByteArray(record, (int) meta.getLength()
              - versionEtc.length(), true);
    } catch (Exception e) {
      // Catch exceptions related to any corrupt archive files.
      return new byte[0];
    }
  }

  /**
   * Extracts contents of the body from an {@code ARCRecord}.
   * Excludes HTTP headers.
   *
   * @param record the {@code ARCRecord}
   * @return contents of the body
   * @throws IOException if there is an issue
   */
  public static byte[] getBodyContent(final ARCRecord record)
      throws IOException {
    byte[] raw = getContent(record);
    int bodyOffset = record.getBodyOffset();

    byte[] content = null;
    try {
      content = new byte[raw.length - bodyOffset];
      System.arraycopy(raw, bodyOffset, content, 0, content.length);
    } catch (java.lang.NegativeArraySizeException e) {
      // To find out what URL causing the error: record.getMetaData().getUrl()
      // For some records, we're missing the actual content data, likely due
      // to a crawler gitch. Nothing much we can do, just swallow and move on.
      content = new byte[0];
    }
    return content;
  }

  /**
   * Copies contents to a byte array.
   *
   * @param is raw input stream
   * @param recordLength is length of a record
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
