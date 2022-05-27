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

import com.google.common.io.Resources
import io.archivesunleashed.udfs.{
  computeImageSize,
  computeMD5,
  computeSHA1,
  extractImageLinks,
  getExtensionMime
}
import org.apache.spark.sql.functions.{desc, explode, unbase64}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class UdfsTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-df"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  test(
    "DF Udf tests; computeSHA1, computeMD5, extractImageLinks, getExtensionMime"
  ) {
    val df = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .keepValidPagesDF()

    // We need this in order to use the $-notation
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val extracted = df
      .select(
        $"url",
        $"mime_type_web_server",
        $"mime_type_tika",
        computeSHA1($"content").as("sha1_test"),
        computeMD5($"content").as("md5_test"),
        explode(extractImageLinks($"url", $"content")).as("image_link"),
        getExtensionMime($"url", $"mime_type_tika").as("extension")
      )
      .orderBy(desc("md5_test"))
      .head(4)
      .toList

    assert(extracted.size == 4)
    assert(
      extracted(0).get(
        0
      ) == "http://www.archive.org/iathreads/post-view.php?id=186011"
    )
    assert(extracted(0).get(1) == "text/html")
    assert(extracted(0).get(2) == "text/html")
    assert(extracted(0).get(3) == "9b9cd08e300f49ae59b1f2ced1bcd43fa8b5418c")
    assert(extracted(0).get(4) == "ff14be99e72943e85fe2368c1e65127a")
    assert(
      extracted(0)
        .get(5)
        .toString == "[http://www.archive.org/iathreads/post-view.php?id=186011,http://www.archive.org/images/logo.jpg,(logo)]"
    )
    assert(extracted(0).get(6) == "html")

    assert(
      extracted(3).get(
        0
      ) == "http://www.archive.org/iathreads/forum-display.php?poster=RipJarvis"
    )
    assert(extracted(3).get(1) == "text/html")
    assert(extracted(3).get(2) == "text/html")
    assert(extracted(3).get(3) == "284a847892deaeb7790fe1b4123a9ccb47a246ed")
    assert(extracted(3).get(4) == "fe0c87b4db0ae846924c56f389083f39")
    assert(
      extracted(3)
        .get(5)
        .toString == "[http://www.archive.org/iathreads/forum-display.php?poster=RipJarvis,http://www.archive.org/images/logo.jpg,(logo)]"
    )
    assert(extracted(3).get(6) == "html")
  }

  test("DF Udf tests; computeImageSize, computeSHA1, computeMD5") {
    val df = RecordLoader
      .loadArchives(arcPath, sc)
      .images()

    // We need this in order to use the $-notation
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val extracted = df
      .select(
        $"md5",
        $"sha1",
        $"height",
        $"width",
        computeImageSize(unbase64($"bytes")).as("image_size"),
        computeSHA1(unbase64($"bytes")).as("sha1_test"),
        computeMD5(unbase64($"bytes")).as("md5_test")
      )
      .withColumn("img_width", $"image_size._1")
      .withColumn("img_height", $"image_size._2")
      .orderBy(desc("md5"))
      .head(2)
      .toList

    assert(extracted.size == 2)
    assert(extracted(0).get(0) == "ff05f9b408519079c992202e8c8a14ee")
    assert(extracted(0).get(1) == "194800d702aab9b8773a1f014e5b611c6af28a97")
    assert(extracted(0).get(2) == 21)
    assert(extracted(0).get(3) == 21)
    assert(extracted(0).get(5) == extracted(0).get(1))
    assert(extracted(0).get(6) == extracted(0).get(0))
    assert(extracted(0).get(7) == extracted(0).get(3))
    assert(extracted(0).get(8) == extracted(0).get(2))

    assert(extracted(1).get(0) == "fbf1aec668101b9601dc0b6de3d0948b")
    assert(extracted(1).get(1) == "564c1a07152c12ceadfad1b9d22013bfcb0f1cab")
    assert(extracted(1).get(2) == 300)
    assert(extracted(1).get(3) == 275)
    assert(extracted(1).get(5) == extracted(1).get(1))
    assert(extracted(1).get(6) == extracted(1).get(0))
    assert(extracted(1).get(7) == extracted(1).get(3))
    assert(extracted(1).get(8) == extracted(1).get(2))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
