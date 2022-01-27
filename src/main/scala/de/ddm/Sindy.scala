package de.ddm

import org.apache.spark.sql.{Dataset, DataFrame, Row, SparkSession}

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

  // Create dataset containing all values of all datasets,
  // paired up with their attribute (column name).
  //
  // | A | B | C |      | value | attribute |
  // |---|---|---|  =>  |-------|-----------|
  // | a | a | b |      |   a   |     A     |
  // | b | a | b |      |   b   |     A     |
  //                    |   a   |     B     |
  //                    |   a   |     B     |
  //                    |  ...       ...    |
  //
  private def readValueAttributePairs(input: String, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val table = readData(input, spark)
    val header = table.columns
    // uncomment to include file name in column name
    //val header = table.columns.map(col => s"$input[$col]")

    table.flatMap(row => row.toSeq.asInstanceOf[Seq[String]].zip(header))
      .withColumnRenamed("_1", "value")
      .withColumnRenamed("_2", "attribute")
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    // we chain the (value, attribute) tables of _all_ datasets
    val pairs = inputs.map(i => readValueAttributePairs(i, spark))
      .reduce((a, b) => a.union(b))
    
    pairs.show()


    // TODO
  }
}
