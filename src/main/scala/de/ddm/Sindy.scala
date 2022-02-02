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
  private def readValueAttributePairs(input: String, spark: SparkSession): Dataset[(String,String)] = {
    import spark.implicits._

    val table = readData(input, spark)
    val header = table.columns
    // uncomment to include file name in column name
    //val header = table.columns.map(col => s"$input[$col]")

    // (a, b, c, d) => (a, A), (b, B), (c, C)
    table.flatMap(row => row.toSeq.asInstanceOf[Seq[String]].zip(header))
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    // see Data Profiling book p.74

    // 1. Read
    // 2. FlatMap
    inputs.map(input => readValueAttributePairs(input, spark)) // read all datasets as (a, A)
      .reduce((ds1, ds2) => ds1.union(ds2)) // append all rows together
      .rdd // we now want to use RDD methods
      .map(t => (t._1, Set(t._2))) // wrap attribute so we can use it in reduceByKey: (a, A) => (a, {A})

      // 3. GroupBy+Aggregate
      .reduceByKey((set1, set2) => set1 ++ set2) // where does the value occur: (a, {A}), (a, {B}) => (a, {A, B})
      .map(t => t._2) // we don't need the value anymore: (a, {A, B}) => {A, B}
      .filter(set => set.size > 1) // drop sets with only 1 element (value is only in 1 column = no INDs are possible)

      // 4. FlatMap
      .flatMap(set => set.map(elem => (elem, set - elem))) // all possible unary INDs: {(A, X\A) for A in X}

      // 5. GroupBy+Aggregate
      .reduceByKey((set1, set2) => set1.intersect(set2)) // (A, {B, C}), (A, {B, D}) => (A, {B})
      .filter(t => t._2.size > 1) // drop entries for which no INDs were found
      
      // 6. Write
      .sortByKey() // sort in lexicographical order by dependent attribute: (A, {...}), (B, {...})
      .foreach(t => println(t._1 + " < " + t._2.mkString(", "))) // print the resulting unary INDs to stdout
  }
}
