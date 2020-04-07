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
package io.archivesunleashed.app

import io.archivesunleashed.{DataFrameLoader}
import org.apache.commons.io.FilenameUtils
import io.archivesunleashed.df.{ComputeImageSizeDF, ComputeMD5DF, ComputeSHA1DF,
                                GetExtensionMimeDF}
import org.apache.hadoop.fs.{Path}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.udf
import java.net.URL
import java.util.Base64

/** Extracts image details given raw bytes. */
object ExtractImageDetailsDF {

  /** Extract Image details from web archive using Data Frame and Spark SQL.
    *
    * @param d DataFrame obtained from RecordLoader
    * @return Dataset[Row], where the schema is (crawl_date, url, filename,
    *   extension, mime_type_server, mime_type_tika, width, height, MD5,
    *   SHA1, body)
    */
  def apply(d: DataFrame): Dataset[Row] = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val width = udf((size: Any) => size.toString.split(",")(0).drop(1))
    val height = udf((size: Any) => size.toString.split(",")(1).dropRight(1))
    val body = udf((bytes: Array[Byte]) => Base64.getEncoder.encodeToString(bytes))
    val filename = udf((url: String) => FilenameUtils.getName(new URL(url).getPath()))
    val urlClass = udf((url: String) => new URL(url).getPath())

    d.select($"crawl_date",
      $"url",
      filename($"url").as("filename"),
      GetExtensionMimeDF(urlClass($"url") ,$"mime_type_tika").as("extension"),
      $"mime_type_web_server",
      $"mime_type_tika",
      width(ComputeImageSizeDF($"bytes")).as("width"),
      height(ComputeImageSizeDF($"bytes")).as("height"),
      ComputeMD5DF($"bytes").as("MD5"),
      ComputeSHA1DF($"bytes").as("SHA1"),
      body($"bytes").as("body"))
  }
}
