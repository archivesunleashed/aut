package io.archivesunleashed.spark.matchbox

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RemoveHttpHeaderTest extends FunSuite {
  test("simple") {
    val header = "HTTP/1.1 200 OK\r\n\r\nHello content"
    val removed = RemoveHttpHeader(header)
    assert(removed == "Hello content")
  }
}
