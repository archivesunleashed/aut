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
import io.archivesunleashed.matchbox.{ComputeImageSize, ComputeMD5RDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{RangePartitioner, SparkContext}
import org.apache.spark.sql.functions.{desc,first}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/** Extract most popular images from an RDD. */
object ExtractPopularImages {
  val LIMIT_MAXIMUM: Int = 500
  val LIMIT_DENOMINATOR: Int = 250
  val MIN_WIDTH: Int = 30
  val MIN_HEIGHT: Int = 30

  /** Extracts the <i>n</i> most popular images from an RDD within a given size range.
   *
   * @param records
   * @param limit number of most popular images in the output
   * @param sc SparkContext
   * @param minWidth of image
   * @param minHeight of image
   */
  def apply(records: RDD[ArchiveRecord], limit: Int, sc:SparkContext, minWidth: Int = MIN_WIDTH, minHeight: Int = MIN_HEIGHT): RDD[String] = {
    val res = records
      .keepImages()
      .map(r => ((r.getUrl, r.getBinaryBytes), 1))
      .map(img => (ComputeMD5RDD(img._1._2), (ComputeImageSize(img._1._2), img._1._1, img._2)))
      .filter(img => img._2._1._1 >= minWidth && img._2._1._2 >= minHeight)
      .reduceByKey((image1, image2) => (image1._1, image1._2, image1._3 + image2._3))
      .map(x=> (x._2._3, x._2._2))
      .takeOrdered(limit)(Ordering[Int].on(x => -x._1))
    val numPartitions = if (limit <= LIMIT_MAXIMUM) 1 else Math.ceil(limit / LIMIT_DENOMINATOR).toInt
    val rdd = sc.parallelize(res)
    rdd.repartitionAndSortWithinPartitions(
      new RangePartitioner(numPartitions, rdd, false)).sortByKey(false).map(x=>x._1 + "\t" + x._2)
  }

  /** Extracts the <i>n</i> most popular images from an Data Frame within a given size range.
   *
   * @param d Data frame obtained from RecordLoader
   * @param limit number of most popular images in the output
   * @param minWidth of image
   * @param minHeight of image
   * @return Dataset[Row], where the schema is (url, count)
   */
  def apply(d: DataFrame, limit: Int, minWidth: Int, minHeight: Int): Dataset[Row] = {

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
