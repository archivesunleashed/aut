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

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.JavaConverters._

// Avoid Hadoop 2.x's winutils-dependent chmod call on this task-local synthetic fixture.
class TaskLocalRawFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
  override def setPermission(
    path: org.apache.hadoop.fs.Path,
    permission: org.apache.hadoop.fs.permission.FsPermission
  ): Unit = ()
}

class TaskLocalFileSystem extends org.apache.hadoop.fs.LocalFileSystem(new TaskLocalRawFileSystem)

@RunWith(classOf[JUnitRunner])
class CommandLineAppCsvHeaderTest extends FunSuite with BeforeAndAfter {
  private var sc: SparkContext = _
  private var tempDir: Path = _

  before {
    sc = new SparkContext(
      new SparkConf()
        .setMaster("local[1]")
        .setAppName("aut-csv-header-test")
        .set("spark.ui.enabled", "false")
        .set("spark.hadoop.fs.file.impl", classOf[TaskLocalFileSystem].getName)
        .set("spark.hadoop.fs.file.impl.disable.cache", "true")
    )
  }

  after {
    if (sc != null) {
      sc.stop()
    }
    if (tempDir != null && Files.exists(tempDir)) {
      FileUtils.deleteDirectory(tempDir.toFile)
    }
  }

  test("CSV output includes the schema header with and without partitioning") {
    tempDir = Files.createTempDirectory(Paths.get("target"), "aut-csv-header-test")
    val httpPayload =
      "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nsynthetic\n"
    val httpPayloadBytes = httpPayload.getBytes(UTF_8)
    val warcHeader =
      "WARC/1.0\r\n" +
        "WARC-Type: response\r\n" +
        "WARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000001>\r\n" +
        "WARC-Date: 2020-01-01T00:00:00Z\r\n" +
        "WARC-Target-URI: http://example.test/synthetic.txt\r\n" +
        "Content-Type: application/http; msgtype=response\r\n" +
        s"Content-Length: ${httpPayloadBytes.length}\r\n\r\n"
    val warcBytes =
      warcHeader.getBytes(UTF_8) ++ httpPayloadBytes ++ "\r\n\r\n".getBytes(UTF_8)
    val input = tempDir.resolve("synthetic.warc")
    Files.write(input, warcBytes)

    val expectedHeader =
      "crawl_date,last_modified_date,url,filename,extension," +
        "mime_type_web_server,mime_type_tika,md5,sha1,content"

    Seq((None, "unpartitioned"), (Some("1"), "partitioned")).foreach {
      case (partition, outputName) =>
        val output = tempDir.resolve(outputName)
        val baseArgs = Seq(
          "--input",
          input.toString,
          "--output",
          output.toString,
          "--extractor",
          "PlainTextInformationExtractor"
        )
        val args = partition match {
          case Some(value) => baseArgs ++ Seq("--partition", value)
          case None        => baseArgs
        }

        CommandLineAppRunner.test(args.toArray, sc)

        val partFiles = Files.list(output)
        val partFile =
          try {
            partFiles.iterator().asScala
              .find(_.getFileName.toString.startsWith("part-"))
              .get
          } finally {
            partFiles.close()
          }
        val lines = Files.readAllLines(partFile, UTF_8)
        assert(lines.get(0) == expectedHeader)
        assert(lines.size() >= 2)
        assert(lines.get(1).contains("synthetic"))
    }
  }
}
