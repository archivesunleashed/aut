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

import java.io.{ByteArrayInputStream, InputStream}

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparklingArchiveRecordTest extends FunSuite {
  private def stream(n: Int): InputStream = new ByteArrayInputStream(Array.fill[Byte](n)(1))

  test("readBounded returns everything when the stream is under the limit") {
    assert(SparklingArchiveRecord.readBounded(stream(10), 100).map(_.length) == Some(10))
  }

  test("readBounded returns everything when the stream is exactly at the limit") {
    assert(SparklingArchiveRecord.readBounded(stream(100), 100).map(_.length) == Some(100))
  }

  test("readBounded returns None when the stream is over the limit") {
    assert(SparklingArchiveRecord.readBounded(stream(101), 100).isEmpty)
  }

  test("readBounded gives up on a stream that never ends") {
    val endless = new InputStream { override def read(): Int = 1 }
    assert(SparklingArchiveRecord.readBounded(endless, 1000).isEmpty)
  }

  // Sparkling decodes Content-Encoding: br bodies with this class. If it goes missing from the
  // classpath, any Brotli-encoded record throws NoClassDefFoundError and fails the whole job.
  test("the Brotli decoder class is on the classpath") {
    Class.forName("org.brotli.dec.BrotliInputStream")
  }
}
