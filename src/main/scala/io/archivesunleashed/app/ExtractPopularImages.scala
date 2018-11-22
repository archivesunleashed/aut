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
package io.archivesunleashed.app

// scalastyle:off underscore.import
import io.archivesunleashed._
// scalastyle:on underscore.import
import io.archivesunleashed.matchbox.{ComputeImageSize, ComputeMD5}
import org.apache.spark.rdd.RDD
import org.apache.spark.{RangePartitioner, SparkContext}

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
      .map(r => ((r.getUrl, r.getImageBytes), 1))
      .map(img => (ComputeMD5(img._1._2), (ComputeImageSize(img._1._2), img._1._1, img._2)))
      .filter(img => img._2._1._1 >= minWidth && img._2._1._2 >= minHeight)
      .reduceByKey((image1, image2) => (image1._1, image1._2, image1._3 + image2._3))
      .map(x=> (x._2._3, x._2._2))
      .takeOrdered(limit)(Ordering[Int].on(x => -x._1))
    val numPartitions = if (limit <= LIMIT_MAXIMUM) 1 else Math.ceil(limit / LIMIT_DENOMINATOR).toInt
    val rdd = sc.parallelize(res)
    rdd.repartitionAndSortWithinPartitions(
      new RangePartitioner(numPartitions, rdd, false)).sortByKey(false).map(x=>x._1 + "\t" + x._2)
  }
}
