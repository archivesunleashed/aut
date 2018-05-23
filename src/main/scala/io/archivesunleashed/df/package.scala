package io.archivesunleashed

import org.apache.spark.sql.functions.udf

/**
  * UDFs for data frames.
  */
package object df {
  // TODO: UDFs for use with data frames go here, tentatively. There are couple of ways we could build UDFs,
  // by wrapping matchbox UDFs or by reimplementing them. The following examples illustrate. Obviously, we'll
  // need to populate more UDFs over time, but this is a start.

  val ExtractDomain = udf(io.archivesunleashed.matchbox.ExtractDomain.apply(_: String, ""))

  val RemovePrefixWWW = udf[String, String](_.replaceAll("^\\s*www\\.", ""))

  var RemoveHTML = udf(io.archivesunleashed.matchbox.RemoveHTML.apply(_:String))
}
