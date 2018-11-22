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
package io.archivesunleashed.matchbox
// scalastyle:off underscore.import
import shapeless._
import syntax.std.tuple._
// scalastyle:on underscore.import
import ops.tuple.FlatMapper
import ops.tuple.ToList
/** Tuple formatter utility. */
object TupleFormatter {
  /** Borrowed from shapeless' flatten.scala example. */
  trait LowPriorityFlatten extends Poly1 {
    // scalastyle:off public.methods.have.type
    implicit def default[T] = at[T](Tuple1(_))
  }

  /** Flattens nested tuples, taking an argument a tuple of any size. */
  // scalastyle:off object.name
  object flatten extends LowPriorityFlatten {
  // scalastyle:on object.name
    implicit def caseTuple[T <: Product](implicit fm: FlatMapper[T, flatten.type]) =
      at[T](_.flatMap(flatten))
  }

  /** Transforms a tuple into a tab-delimited string, flattening any nesting,
    * taking an argument a tuple of any size. */
  // scalastyle:off object.name
  object tabDelimit extends Poly1 {
  // scalastyle:on object.name
    implicit def caseTuple[T <: Product, Lub](implicit tl: ToList[T, Lub], fm: FlatMapper[T, flatten.type]) =
      at[T](flatten(_).asInstanceOf[Product].productIterator.mkString("\t"))
  }
  // scalastyle:on public.methods.have.type
}
