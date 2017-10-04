package io.archivesunleashed.spark.matchbox

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RemoveHTMLTest extends FunSuite {
  test("simple") {
    val html =
      """
      <html>
      <body>
      <div>Here is some...</div>
      <p>HTML</p>
      </body>
      </html>
      """
    val removed = RemoveHTML(html)
    assert(removed == "Here is some... HTML")
  }
}
