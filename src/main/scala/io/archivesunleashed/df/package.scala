package io.archivesunleashed

import org.apache.spark.sql.functions.udf

/**
  * Created by jimmylin on 4/9/18.
  */
package object df {
  val ExtractDomain = udf(io.archivesunleashed.matchbox.ExtractDomain.apply(_: String, ""))
}
