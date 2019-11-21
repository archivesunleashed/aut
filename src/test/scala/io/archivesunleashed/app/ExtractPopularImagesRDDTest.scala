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

import com.google.common.io.Resources
import io.archivesunleashed.RecordLoader
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class ExtractPopularImagesRDDTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private var sc: SparkContext = _
  private val master = "local[4]"
  private val appName = "example-spark"
  private val testVertexFile = "temporaryTestVertexDir"
  private val testEdgesFile = "temporaryTestEdgesDir"

  before {
    val conf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true");
    sc = new SparkContext(conf)
  }

  test("extracts popular images") {
    val highTest = 507
    val examplerdd = RecordLoader.loadArchives(arcPath, sc)
    val imagesLowLimit = ExtractPopularImagesRDD(examplerdd, 3, sc)
    val imagesHighLimit = ExtractPopularImagesRDD(examplerdd, highTest, sc)
    val response = Array("1\thttp://www.archive.org/images/books-small.jpg",
      "1\thttp://i.creativecommons.org/l/by-sa/3.0/88x31.png",
      "1\thttp://www.archive.org/images/blendbar.jpg")
    assert (imagesLowLimit.take(3).deep == response.deep)
    assert (imagesHighLimit.take(3).deep == response.deep)
  }
  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
