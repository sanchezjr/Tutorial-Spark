package com.operacionesDF
import com.utils.Context
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
object OperacionesDF extends App with Context
{

  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)

  val dfQuestionsCSV = sparkSession
    .read
    .option("header", false)
    .option("inferSchema", true)
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  val dfQuestions = dfQuestionsCSV
    .filter("score > 400 and score < 410")
    .join(dfTags, "id")
    .select("owner_userid", "tag", "creation_date", "score")
    .toDF()

  dfQuestions.show(10)

  //Convert DataFrame row to Scala case class

  //first create a case class to represent the tag properties namely id and tag.
  case class Tag(id: Int, tag: String)

  //convert each row of the dataframe dfTags into Scala case class Tag created above.
  import sparkSession.implicits._
  val dfTagsOfTag: Dataset[Tag] = dfTags.as[Tag]
  dfTagsOfTag
    .take(10)
    .foreach(t => println(s"id = ${t.id}, tag = ${t.tag}"))


  //DataFrame row to Scala case class using map()

  //create a Case Class to represent the StackOverflow question dataset.
  case class Question(owner_userid: Int, tag: String, creationDate: java.sql.Timestamp, score: Int)

  // create a function which will parse each element in the row
  def toQuestion(row: org.apache.spark.sql.Row): Question = {
    // to normalize our owner_userid data
    val IntOf: String => Option[Int] = _ match {
      case s if s == "NA" => None
      case s => Some(s.toInt)
    }


    import java.time._
    val DateOf: String => java.sql.Timestamp = _ match {
      case s => java.sql.Timestamp.valueOf(ZonedDateTime.parse(s).toLocalDateTime)
    }

    Question (
      owner_userid = IntOf(row.getString(0)).getOrElse(-1),
      tag = row.getString(1),
      creationDate = DateOf(row.getString(2)),
      score = row.getString(3).toInt
    )
  }

  val dfOfQuestion: Dataset[Question] = dfQuestions.map(row => toQuestion(row))
  dfOfQuestion
    .take(10)
    .foreach(q => println(s"owner userid = ${q.owner_userid}, tag = ${q.tag}, creation date = ${q.creationDate}, score = ${q.score}"))

  //Create DataFrame from collection

  import sparkSession.implicits._
  val seqTags = Seq(
    1 -> "so_java_3",
    1 -> "so_jsp_3",
    2 -> "so_erlang_3",
    3 -> "so_scala_3",
    3 -> "so_akka_3"
  )

  val dfMoreTags = seqTags.toDF("id", "tag")
  dfMoreTags.show(10)

  //DataFrame Union
  //this example, merge the dataframe dfTags and the dataframe dfMoreTags
  // which we created from the previous section.

  val dfUnionOfTags = dfTags
    .union(dfMoreTags)
    .filter("id in (1,3)")
  dfUnionOfTags.show(10)

  //DataFrame Intersection
  val dfIntersectionTags = dfMoreTags
    .intersect(dfUnionOfTags)
    .show(10)

  //Append column to DataFrame using withColumn()
  val dfSplitColumn = dfMoreTags
    .withColumn("tmp", split($"tag", "_"))
    .select(
      $"id",
      $"tag",
      $"tmp".getItem(0).as("so_prefix"),
      $"tmp".getItem(2).as("so_tag"),
      $"tmp"
    )
  dfSplitColumn.show(10)

  //Create DataFrame from Tuples
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")

  df.show()

  //Get DataFrame column names
  val columnNames: Array[String] = df.columns
  columnNames.foreach(name => println(s"$name"))

  //DataFrame column names and types
  val (columnNames2, columnDataTypes) = df.dtypes.unzip
  println(s"DataFrame column names = ${columnNames2.mkString(", ")}")
  println(s"DataFrame column data types = ${columnDataTypes.mkString(", ")}")
}
