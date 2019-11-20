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

import io.archivesunleashed.ArchiveRecord
import org.apache.spark.{RangePartitioner, SparkContext}
import org.apache.spark.sql.functions.{desc,first}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/** Extract most popular images from a Data Frame. */
object ExtractPopularImages {
  val MIN_WIDTH: Int = 30
  val MIN_HEIGHT: Int = 30

  /** Extracts the <i>n</i> most popular images from an Data Frame within a given size range.
   *
   * @param d Data frame obtained from RecordLoader
   * @param limit number of most popular images in the output
   * @param minWidth of image
   * @param minHeight of image
   * @return Dataset[Row], where the schema is (url, count)
   */
  def apply(d: DataFrame, limit: Int, minWidth: Int = MIN_WIDTH, minHeight: Int = MIN_HEIGHT): Dataset[Row] = {

      val spark = SparkSession.builder().master("local").getOrCreate()
      // scalastyle:off
      import spark.implicits._
      // scalastyle:on

      val df = d.select($"url",$"md5")
                .filter(($"width") >= minWidth && ($"height") >= minHeight)

      val count = df.groupBy("md5").count()

      df.join(count,"md5")
        .groupBy("md5")
        .agg(first("url").as("url"), first("count").as("count"))   
        .select("url","count")    
        .orderBy(desc("count"))
        .limit(limit)
  }
}
