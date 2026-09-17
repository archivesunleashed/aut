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

package io.archivesunleashed.matchbox

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class DetectMimeTypeTikaTest extends FunSuite {
  test("Detects MIME type from bytes") {
    assert(DetectMimeTypeTika(Array.empty[Byte]) == "N/A")
    assert(
      DetectMimeTypeTika(
        "<html><body>hi</body></html>".getBytes("UTF-8")
      ) == "text/html"
    )
    assert(
      DetectMimeTypeTika(
        "just some plain text, nothing more".getBytes("UTF-8")
      ) == "text/plain"
    )
  }

  // DetectMimeTypeTika reuses a single Tika/AutoDetectParser/DefaultDetector
  // instance across every call (see DetectMimeTypeTika.scala), rather than
  // constructing one per invocation. Under Sparkling's async record
  // pipeline, that singleton can be invoked from several threads at once
  // within one executor JVM, so this hammers it concurrently across many
  // threads and distinct inputs to check it stays correct under contention.
  test(
    "Shared Tika singleton produces correct, stable results under concurrent load"
  ) {
    val samples: Seq[(Array[Byte], String)] = Seq(
      "<html><body>hi</body></html>".getBytes("UTF-8") -> "text/html",
      "just some plain text, nothing more".getBytes(
        "UTF-8"
      ) -> "text/plain",
      (Array[Byte](0x89.toByte, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a) ++
        Array.fill[Byte](16)(0)) -> "image/png",
      "%PDF-1.4\n%some pdf content\n".getBytes("UTF-8") -> "application/pdf",
      ("GIF89a".getBytes("UTF-8") ++ Array.fill[Byte](16)(0)) -> "image/gif"
    )

    implicit val ec: ExecutionContext = ExecutionContext.global
    val iterationsPerSample = 200

    val futures = for {
      (bytes, expected) <- samples
      _ <- 1 to iterationsPerSample
    } yield Future {
      val detected = DetectMimeTypeTika(bytes)
      assert(
        detected == expected,
        s"expected $expected but got $detected under concurrent load"
      )
    }

    Await.result(Future.sequence(futures), 2.minutes)
  }
}
