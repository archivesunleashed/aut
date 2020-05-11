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

import io.archivesunleashed.matchbox.{ComputeImageSize, ComputeMD5RDD, ComputeSHA1RDD,
                                      DetectLanguageRDD, DetectMimeTypeTika,
                                      ExtractBoilerpipeTextRDD, ExtractDateRDD,
                                      ExtractDomainRDD, ExtractImageLinksRDD, ExtractLinksRDD,
                                      GetExtensionMimeRDD, RemoveHTMLRDD, RemoveHTTPHeaderRDD}
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

  val ComputeImageSizeDF = udf(ComputeImageSize.apply(_: Array[Byte]))

  val ComputeMD5DF = udf(ComputeMD5RDD.apply(_: Array[Byte]))

  val ComputeSHA1DF = udf(ComputeSHA1RDD.apply(_: Array[Byte]))

  val DetectLanguageDF = udf(DetectLanguageRDD.apply(_: String))

  val DetectMimeTypeTikaDF = udf((DetectMimeTypeTika.apply(_: Array[Byte])))

  val ExtractBoilerpipeTextDF = udf(ExtractBoilerpipeTextRDD.apply(_: String))

  val ExtractDateDF = udf((ExtractDateRDD.apply(_: String, _: String)))

  val ExtractDomainDF = udf(ExtractDomainRDD.apply(_: String, ""))

  val ExtractImageLinksDF = udf(ExtractImageLinksRDD.apply(_: String, _: String))

  val ExtractLinksDF = udf(ExtractLinksRDD.apply(_: String, _: String))

  val GetExtensionMimeDF = udf(GetExtensionMimeRDD.apply(_: String, _: String))

  val hasContent = udf((c: String, contentREs: Seq[String]) => {
    contentREs.map(re =>
        (re.r findFirstIn c) match {
          case Some(v) => true
          case None => false
        }).exists(identity)
  })

  val hasDate = udf((date_ : String, date: Seq[String]) => date.contains(date_))

  val hasDomains = udf((domain: String, domains: Seq[String]) => domains.contains(domain))

  val hasHTTPStatus = udf((statusCode: String, statusCodes: Seq[String]) => statusCodes.contains(statusCode))

  val hasImages = udf((date: String, mimeType: String) => date != null && mimeType.startsWith("image/"))

  val hasLanguages = udf((language: String, languages: Seq[String]) => languages.contains(language))

  val hasMIMETypes = udf((mimeType: String, mimeTypes: Seq[String]) => mimeTypes.contains(mimeType))

  val hasMIMETypesTika = udf((mimeType: String, mimeTypesTika: Seq[String]) => mimeTypesTika.contains(mimeType))

  val hasUrlPatterns = udf((urlPattern: String, urlREs: Seq[String]) => {
    urlREs.map(re =>
        urlPattern match {
          case re.r() => true
          case _ => false
        }).exists(identity)
  })

  val hasUrls = udf((url: String, urls: Seq[String]) => urls.contains(url))

  val RemoveHTTPHeaderDF = udf(RemoveHTTPHeaderRDD.apply(_: String))

  val RemovePrefixWWWDF = udf[String, String](_.replaceAll("^\\s*www\\.", ""))

  var RemoveHTMLDF = udf(RemoveHTMLRDD.apply(_: String))

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
