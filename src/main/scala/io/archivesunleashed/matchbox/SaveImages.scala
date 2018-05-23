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
package io.archivesunleashed.matchbox

import org.apache.spark.sql.DataFrame

import java.io.ByteArrayInputStream
import java.io.File
import javax.imageio.{ImageIO, ImageReader}
import java.util.Base64

/** Save images to disk from a dataframe */
object SaveImages {

  /** 
   * Given a dataframe, serializes the images and saves to disk
   * @param df the input dataframe
   * @param bytesColumnName the name of the column containing the image bytes
   * @param fileName the name of the file to save the images to (without extension)
   * e.g. fileName = "foo" => images are saved as foo0.jpg, foo1.jpg
  */
  def apply(df: DataFrame, bytesColumnName: String, fileName: String) = {
    var count = 0;
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
          val file = new File(fileName + count + "." + format);
          count += 1
          if (image != null) {
            ImageIO.write(image, format, file);
          }
        }
        
      } catch {
        case e: Throwable => {}
      }
    })
  }
}