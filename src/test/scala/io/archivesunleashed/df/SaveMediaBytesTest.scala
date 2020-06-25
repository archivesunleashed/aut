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
import io.archivesunleashed.df.SaveBytes
import io.archivesunleashed.matchbox.ComputeMD5
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.File
import java.nio.file.{Paths, Files}
import java.util.Base64

case class TestMediaDetails(
    url: String,
    extension: String,
    mime_type: String,
    md5: String,
    bytes: String
)

@RunWith(classOf[JUnitRunner])
class SaveMediaBytesTest extends FunSuite with BeforeAndAfter {
  private val warcPath =
    Resources.getResource("warc/example.media.warc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-df"
  private var sc: SparkContext = _
  private val testString = "bytes"
  private val testExtension = "extension"

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  test("Save audio bytes to disk DF") {
    val df = RecordLoader
      .loadArchives(warcPath, sc)
      .audio()

    val extracted = df
      .select(testString, testExtension)
      .orderBy(desc(testString))
      .limit(1)
    extracted.saveToDisk(testString, "/tmp/audio", testExtension)

    val encodedBytes: String = extracted.take(1)(0).getAs(testString)
    val bytes = Base64.getDecoder.decode(encodedBytes);

    val suffix = ComputeMD5(bytes)
    val fileName = "/tmp/audio-" + suffix + ".mp3"
    assert(Files.exists(Paths.get(fileName)))

    Files.delete(Paths.get(fileName))
  }

  test("Attempt to save invalid audio DF") {
    val dummyEncBytes = Base64.getEncoder.encodeToString(
      Array
        .range(0, 127)
        .map(_.toByte)
    )
    val dummyMD5 = ComputeMD5(dummyEncBytes.getBytes)
    val dummyAudio = TestMediaDetails(
      "http://example.com/fake.mp3",
      "mp3",
      "audio/mpeg",
      dummyMD5,
      dummyEncBytes
    )

    // For toDF().
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val df = Seq(dummyAudio).toDF

    df.saveToDisk(testString, "/tmp/bar", "extension")

    // Check that no file was written.
    assert(
      new File("/tmp").listFiles
        .filter(_.isFile)
        .toList
        .count(_.getName.startsWith("bar-" + dummyMD5)) == 0
    )
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
