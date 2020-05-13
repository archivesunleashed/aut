/*
 * Copyright © 2017 The Archives Unleashed Project
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

import io.archivesunleashed.matchbox.ComputeMD5RDD
import java.io.ByteArrayInputStream
import java.io.FileOutputStream
import java.util.Base64
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.DataFrame

package object df {

  /**
   * Given a dataframe, serializes binary object and saves to disk
   * @param df the input dataframe
   */
  implicit class SaveBytes(df: DataFrame) {

    /**
      * @param bytesColumnName the name of the column containing the bytes
      * @param fileName the name of the file to save the binary file to (without extension)
      * @param extensionColumnName the name of the column containin the extension
      * e.g. fileName = "foo" => files are saved as "foo-[MD5 hash].pdf"
      */
    def saveToDisk(bytesColumnName: String, fileName: String, extensionColumnName: String): Unit = {
      df.select(bytesColumnName, extensionColumnName).foreach(row => {
        try {
          // Assumes the bytes are base64 encoded.
          val encodedBytes: String = row.getAs(bytesColumnName);
          val bytes = Base64.getDecoder.decode(encodedBytes);
          val in = new ByteArrayInputStream(bytes);

          val extension: String = row.getAs(extensionColumnName);
          val suffix = ComputeMD5RDD(bytes)
          val file = new FileOutputStream(fileName + "-" + suffix + "." + extension.toLowerCase)
          IOUtils.copy(in, file)
          file.close()
        } catch {
          case e: Throwable => {
          }
        }
      })
    }
  }
}
