package org.apache.flink.quickstart

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

object ReplyGraph extends App {
  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  val autoMails = List("git@git.apache.org", "Atlassian.JIRA")

  // Tuple3 with (MessageID, Sender, Reply-To)
  val input = env.readCsvFile[(String, String, String)](
    "input/flinkMails.del",
    lineDelimiter = "##//##",
    fieldDelimiter = "#|#",
    includedFields = Array(0, 2, 5)
  )


val mails =
  input
    // filter out the emails that are not a reply to another mail
    .map(e => (e._1, e._2.split("<")(1).replace(">", ""), e._3))
    .filter { m => !(m._2.equals("git@git.apache.org") || m._2.equals("jira@apache.org")) }

val replies = mails.join(mails).where(0).equalTo(2) {(e1, e2) => (e1._2, e2._2, 1)}
  .groupBy(0,1)
  .sum(2)
  .setParallelism(1)
  .sortPartition(2, Order.ASCENDING)
  .print()

}
