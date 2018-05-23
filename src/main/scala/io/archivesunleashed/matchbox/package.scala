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

package io.archivesunleashed

import java.io.IOException
import java.security.MessageDigest
import java.io.ByteArrayInputStream
import java.io.File
import javax.imageio.{ImageIO, ImageReader}
import java.util.Base64

import scala.xml.Utility._

import org.apache.spark.sql.DataFrame

/** Package object which supplies implicits providing common UDF-related functionalities. */
package object matchbox {
  implicit class WWWLink(s: String) {
    def removePrefixWWW(): String = {
      if (s == null) return null
      s.replaceAll("^\\s*www\\.", "")
    }

    def escapeInvalidXML(): String = {
      try {
        return escape(s)
      }
      catch {
        case e: Exception => throw new IOException("Caught exception processing input row ", e)
      }
    }

    def computeHash(): String = {
      val md5 = MessageDigest.getInstance("MD5")
      return md5.digest(s.getBytes).map("%02x".format(_)).mkString
    }
  }

  /**
   * Given a dataframe, serializes the images and saves to disk
   * @param df the input dataframe
  */
  implicit class SaveImage(df: DataFrame) {
    /** 
     * @param bytesColumnName the name of the column containing the image bytes
     * @param fileName the name of the file to save the images to (without extension)
     * e.g. fileName = "foo" => images are saved as foo0.jpg, foo1.jpg
    */
    def saveToDisk(bytesColumnName: String, fileName: String) = {
      df.select(bytesColumnName).foreach(row => {
        try {
          // assumes the bytes are base64 encoded already as returned by ExtractImageDetails
          val encodedBytes: String = row.getAs(bytesColumnName);
          val bytes = Base64.getDecoder.decode(encodedBytes);
          val in = new ByteArrayInputStream(bytes);

          val input = ImageIO.createImageInputStream(in);
          val readers = ImageIO.getImageReaders(input);
          if (readers.hasNext()) {
            val reader = readers.next()
            reader.setInput(input)
            val image = reader.read(0)

            val format = reader.getFormatName()
            val suffix = encodedBytes.computeHash()
            val file = new File(fileName + "-" + suffix + "." + format);
            if (image != null) {
              ImageIO.write(image, format, file);
            }
          }
          row
        } catch {
          case e: Throwable => {
            row
          }
        }
      })
    }
  }
}
