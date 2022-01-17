package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    for (a <- inputs) {
      val table = readData(a, spark)
      val header= table.columns
      //table.map(row => header.zipWithIndex.map((t) =>row.getString(t._2)))//.flatten.map((t) => (t._1, Set(t._2))).reduceByKey((a, b) => a ++ b)
    }

    // TODO
  }
}
