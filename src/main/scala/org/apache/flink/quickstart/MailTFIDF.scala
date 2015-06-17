package org.apache.flink.quickstart

import java.util.regex.Pattern

import org.apache.flink.api.scala._

object MailTFIDF extends App {

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // Tuple3 with (MessageID, Body)
  val input = env.readCsvFile[(String, String)](
    "input/flinkMails.del",
    lineDelimiter = "##//##",
    fieldDelimiter = "#|#",
    includedFields = Array(0, 4)
  )

  val mailCount = input.count()

  // compute term-frequency (TF)
  val tf = input
    .flatMap { x =>
      words(x._2).foldLeft(Map.empty[String, Int]) {
        (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
    }.map { case(word, count) =>
      (x._1, word, count)
    }
  }

  // compute document frequency (number of mails that contain a word at least once)
  val df = input
    .flatMap { x => words(x._2).distinct.map( word => (word, 1)) }
    // count number of mails for each word
    .groupBy(0)
    .reduce { (l,r) => (l._1, l._2 + r._2) }

  // compute TF-IDF score from TF, DF, and total number of mails
  val tfidf = tf
    .join(df)
    .where(1)
    .equalTo(0)
      { (l, r) => (l._1, l._2, l._3 * (mailCount.toDouble / r._2) ) }

  // print the result
  tfidf
    .print()

  def words(body: String) = {
    val STOP_WORDS: Array[String] = Array(
      "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is", "there", "it", "this",
      "that", "on", "was", "by", "of", "to", "in", "to", "message", "not", "be", "with", "you",
      "have", "as", "can")
    // initalize pattern to match words
    val wordPattern: Pattern = Pattern.compile("(\\p{Alpha})+")

    // split mail along whitespaces
    val tokens = body.split(" ")

    tokens
      .filter(x => !STOP_WORDS.contains(x) && wordPattern.matcher(x).matches())
  }

}
