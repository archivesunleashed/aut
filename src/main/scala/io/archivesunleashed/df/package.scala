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

import org.apache.commons.io.IOUtils
import io.archivesunleashed.matchbox.{ComputeMD5}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrame
import java.io.ByteArrayInputStream
import java.io.FileOutputStream
import java.util.Base64

/**
  * UDFs for data frames.
  */
package object df {
  // UDFs for use with data frames go here, tentatively. There are couple of ways we could build UDFs,
  // by wrapping matchbox UDFs or by reimplementing them. The following examples illustrate. Obviously, we'll
  // need to populate more UDFs over time, but this is a start.

  val ExtractDomain = udf(io.archivesunleashed.matchbox.ExtractDomain.apply(_: String, ""))

  val RemoveHTTPHeader = udf(io.archivesunleashed.matchbox.RemoveHTTPHeader.apply(_: String))

  val RemovePrefixWWW = udf[String, String](_.replaceAll("^\\s*www\\.", ""))

  var RemoveHTML = udf(io.archivesunleashed.matchbox.RemoveHTML.apply(_: String))

  val ExtractLinks = udf(io.archivesunleashed.matchbox.ExtractLinks.apply(_: String, _: String))

  val GetExtensionMime = udf(io.archivesunleashed.matchbox.GetExtensionMime.apply(_: String, _: String))

  val ExtractImageLinks = udf(io.archivesunleashed.matchbox.ExtractImageLinks.apply(_: String, _: String))

  val ComputeMD5DF = udf((content: String) => io.archivesunleashed.matchbox.ComputeMD5.apply(content.getBytes()))
  
  val ComputeSHA1DF = udf((content: String) => io.archivesunleashed.matchbox.ComputeSHA1.apply(content.getBytes()))

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
          val suffix = ComputeMD5(bytes)
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
