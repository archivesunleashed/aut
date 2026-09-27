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

@RunWith(classOf[JUnitRunner])
class CovertLastModifiedDateTest extends FunSuite {
  test("Converts an RFC 1123 date to yyyyMMddHHmmss") {
    assert(CovertLastModifiedDate("Tue, 15 Nov 1994 08:12:31 GMT") == "19941115081231")
    assert(CovertLastModifiedDate("Fri, 20 Jul 2018 19:09:28 GMT") == "20180720190928")
  }

  test("Matches month names case-insensitively") {
    assert(CovertLastModifiedDate("MON, 01 DEC 2014 00:00:00 GMT") == "20141201000000")
  }

  test("Returns an empty string for an empty date") {
    assert(CovertLastModifiedDate("") == "")
  }

  test("Returns an empty string when no month name is present") {
    assert(CovertLastModifiedDate("not a date") == "")
  }

  test("Returns an empty string when a month is present but other parts are missing") {
    assert(CovertLastModifiedDate("Tue, 15 Nov") == "")
  }
}
