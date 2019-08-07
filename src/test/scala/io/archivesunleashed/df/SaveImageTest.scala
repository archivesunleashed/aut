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

package io.archivesunleashed

import com.google.common.io.Resources
// scalastyle:off underscore.import
import io.archivesunleashed.df._
import org.apache.spark.sql.functions._
import io.archivesunleashed.matchbox._
// scalastyle:on underscore.import
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.{Paths, Files}
import java.io.{File, ByteArrayInputStream}
import javax.imageio.ImageIO
import java.util.Base64

case class TestImageDetails(url: String, mime_type: String, width: String,
                        height: String, md5: String, bytes: String)

@RunWith(classOf[JUnitRunner])
class SaveImageTest extends FunSuite with BeforeAndAfter {
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

  test("Save image") {
    val testString = "bytes"
    val df = RecordLoader.loadArchives(arcPath, sc)
      .extractImageDetailsDF()

    val extracted = df.select("bytes")
      .orderBy(desc(testString)).limit(1)
    extracted.saveToDisk(testString, "/tmp/foo")

    val encodedBytes: String = extracted.take(1)(0).getAs(testString)
    val bytes = Base64.getDecoder.decode(encodedBytes);

    val suffix = ComputeMD5(bytes)
    val fileName = "/tmp/foo-" + suffix + ".png"
    assert(Files.exists(Paths.get(fileName)))

    val originalBytes = Base64.getDecoder.decode(encodedBytes)
    val bis = new ByteArrayInputStream(originalBytes)
    val originalImage = ImageIO.read(bis)

    val savedImage = ImageIO.read(new File(fileName))

    assert(originalImage.getHeight() == savedImage.getHeight())
    assert(originalImage.getWidth() == savedImage.getWidth())
    for {y <- 0 until originalImage.getHeight()} {
      for (x <- 0 until originalImage.getWidth()) {
        assert(originalImage.getRGB(x, y) == savedImage.getRGB(x, y))
      }
    }
    Files.delete(Paths.get(fileName))
  }

  test("Attempt to save invalid image") {
    val dummyEncBytes = Base64.getEncoder.encodeToString(Array.range(0, 127)
      .map(_.toByte))
    val dummyMD5 = ComputeMD5(dummyEncBytes.getBytes)
    val dummyImg = TestImageDetails("http://example.com/fake.jpg", "image/jpeg",
      "600", "800", dummyMD5, dummyEncBytes)

    // For toDF().
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    val df = Seq(dummyImg).toDF

    df.saveToDisk("bytes", "/tmp/bar")

    // Check that no file was written.
    assert(new File("/tmp").listFiles.filter(_.isFile).toList
      .count(_.getName.startsWith("bar-" + dummyMD5)) == 0)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
