package com.estadistica

import com.utils.Context
import org.apache.spark.sql.functions._
object Tutorial_df_estadistica extends App with Context{
  // Create a dataframe from tags file question_tags_10K.csv
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  // Create a dataframe from questions file questions_10K.csv
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  // cast columns to data types
  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

  // Average
  dfQuestions
    .select(avg("score"))
    .show()

  // Max
  dfQuestions
    .select(max("score"))
    .show()

  // Minimum
  dfQuestions
    .select(min("score"))
    .show()

  // Mean
  dfQuestions
    .select(mean("score"))
    .show()

  // Sum
  dfQuestions
    .select(sum("score"))
    .show()


  // Group by with statistics
  dfQuestions
    .filter("id > 400 and id < 450")
    .filter("owner_userid is not null")
    .join(dfTags, dfQuestions.col("id").equalTo(dfTags("id")))
    .groupBy(dfQuestions.col("owner_userid"))
    .agg(avg("score"), max("answer_count"))
    .show()


  // DataFrame Statistics using describe() method
  val dfQuestionsStatistics = dfQuestions.describe()
  dfQuestionsStatistics.show()


  // Correlation
  val correlation = dfQuestions.stat.corr("score", "answer_count")
  println(s"correlation between column score and answer_count = $correlation")

  // Covariance
  val covariance = dfQuestions.stat.cov("score", "answer_count")
  println(s"covariance between column score and answer_count = $covariance")

  // Frequent Items
  val dfFrequentScore = dfQuestions.stat.freqItems(Seq("answer_count"))
  dfFrequentScore.show()

  // Crosstab
  val dfScoreByUserid = dfQuestions
    .filter("owner_userid > 0 and owner_userid < 20")
    .stat
    .crosstab("score", "owner_userid")
  dfScoreByUserid.show(10)


  // find all rows where answer_count in (5, 10, 20)
  val dfQuestionsByAnswerCount = dfQuestions
    .filter("owner_userid > 0")
    .filter("answer_count in (5, 10, 20)")

  // count how many rows match answer_count in (5, 10, 20)
  dfQuestionsByAnswerCount
    .groupBy("answer_count")
    .count()
    .show()
}
