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

import io.archivesunleashed.matchbox.{ComputeMD5RDD}
import java.io.ByteArrayInputStream
import java.io.FileOutputStream
import java.util.Base64
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import scala.util.matching.Regex

/**
  * UDFs for DataFrames.
  */
package object df {
  // UDFs for use with DataFrames go here, tentatively. There are couple of ways we could build UDFs,
  // by wrapping matchbox UDFs or by reimplementing them. The following examples illustrate. Obviously, we'll
  // need to populate more UDFs over time, but this is a start.

  val ExtractDomainDF = udf(io.archivesunleashed.matchbox.ExtractDomainRDD.apply(_: String, ""))

  val RemoveHTTPHeaderDF = udf(io.archivesunleashed.matchbox.RemoveHTTPHeaderRDD.apply(_: String))

  val RemovePrefixWWWDF = udf[String, String](_.replaceAll("^\\s*www\\.", ""))

  var RemoveHTMLDF = udf(io.archivesunleashed.matchbox.RemoveHTMLRDD.apply(_: String))

  val ExtractLinksDF = udf(io.archivesunleashed.matchbox.ExtractLinksRDD.apply(_: String, _: String))

  val GetExtensionMimeDF = udf(io.archivesunleashed.matchbox.GetExtensionMimeRDD.apply(_: String, _: String))

  val ExtractImageLinksDF = udf(io.archivesunleashed.matchbox.ExtractImageLinksRDD.apply(_: String, _: String))

  val ComputeMD5DF = udf(io.archivesunleashed.matchbox.ComputeMD5RDD.apply(_: Array[Byte]))

  val ComputeSHA1DF = udf(io.archivesunleashed.matchbox.ComputeSHA1RDD.apply(_: Array[Byte]))

  val ComputeImageSizeDF = udf(io.archivesunleashed.matchbox.ComputeImageSize.apply(_: Array[Byte]))

  val DetectLanguageDF = udf(io.archivesunleashed.matchbox.DetectLanguageRDD.apply(_: String))

  val ExtractBoilerpipeTextDF = udf(io.archivesunleashed.matchbox.ExtractBoilerpipeTextRDD.apply(_: String))

  val ExtractDateDF = udf((io.archivesunleashed.matchbox.ExtractDateRDD.apply(_: String, _: String)))

  val DetectMimeTypeTikaDF = udf((io.archivesunleashed.matchbox.DetectMimeTypeTika.apply(_: Array[Byte])))

  val hasHTTPStatus = udf((statusCode: String, statusCodes: Seq[String]) => statusCodes.contains(statusCode))

  val hasMIMETypes = udf((mimeType: String, mimeTypes: Seq[String]) => mimeTypes.contains(mimeType))

  val hasMIMETypesTika = udf((mimeType: String, mimeTypesTika: Seq[String]) => mimeTypesTika.contains(mimeType))

  val hasDate = udf((date_ : String, date: Seq[String]) => date.contains(date_))

  val hasUrls = udf((url: String, urls: Seq[String]) => urls.contains(url))

  val hasDomains = udf((domain: String, domains: Seq[String]) => domains.contains(domain))

  val hasImages = udf((date: String, mimeType: String) => date != null && mimeType.startsWith("image/"))

  val hasContent = udf((c: String, contentREs: Seq[String]) => {
    contentREs.map(re =>
        (re.r findFirstIn c) match {
          case Some(v) => true
          case None => false
        }).exists(identity)
  })

  val hasUrlPatterns = udf((urlPattern: String, urlREs: Seq[String]) => {
    urlREs.map(re =>
        urlPattern match {
          case re.r() => true
          case _ => false
        }).exists(identity)
  })

  val hasLanguages = udf((language: String, languages: Seq[String]) => languages.contains(language))

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
