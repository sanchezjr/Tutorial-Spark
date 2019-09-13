package com.dataframe
import com.utils.Context
import org.apache.spark.sql.functions._
object Tutorial_dataframe extends App with Context {

  // Create a DataFrame from reading a CSV file
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)

  // Print DataFrame schema
  dfTags.printSchema()

  // Query dataframe: select columns from a dataframe
  dfTags.select("id", "tag").show(10)

  // DataFrame Query: filter by column value of a dataframe
  dfTags.filter("tag == 'python'").show(10)

  // DataFrame Query: count rows of a dataframe
  println(s"Number of php tags = ${ dfTags.filter("tag == 'python'").count() }")

  // DataFrame Query: SQL like query (words begin with 's')
  dfTags.filter("tag like 's%'").show()

  // DataFrame Query: Multiple filter chaining (find all tags whose value starts with letter s and then only pick id 25 or 108.)
  dfTags
    .filter("tag like 's%'")
    .filter("id == 25 or id == 108")
    .show(10)

  // DataFrame Query: SQL IN clause (find all tags whose ids are equal to (25, 108))
  dfTags.filter("id in (25, 108)").show(10)

  // DataFrame Query: SQL Group By
  println("Group by tag value")
  dfTags.groupBy("tag").count().show(10)

  // DataFrame Query: SQL Group By with filter
  dfTags.groupBy("tag").count().filter("count > 5").show(10)

  // DataFrame Query: SQL order by
  dfTags.groupBy("tag").count().filter("count > 5").orderBy(desc("count")).show(10)
}
