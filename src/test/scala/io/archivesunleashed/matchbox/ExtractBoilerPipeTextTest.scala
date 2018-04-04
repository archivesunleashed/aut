/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source platform for analyzing web archives.
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
 import java.io.IOException

 @RunWith(classOf[JUnitRunner])
 class ExtractBoilerpipeTextTest extends FunSuite {
   var text = """<p>Text with a boiler plate.<p>
   <footer>Copyright 2017</footer>"""
   var boiler = """Copyright 2017"""
   test ("Collects boilerpip") {
     assert (ExtractBoilerpipeText (text) == boiler)
     assert (ExtractBoilerpipeText ("") == Nil)
     assert (ExtractBoilerpipeText ("All Rights Reserved.") == Nil)
     val caught = intercept[IOException] {ExtractBoilerpipeText (null)}
     assert (caught.getMessage == "Caught exception processing input row java.lang.NullPointerException")
   }
 }
