package notebook.kernel.test

import notebook.kernel.Repl
import org.scalatest.FlatSpec

/**
  * @author : tong.wang
  * @since : 10/8/18 11:02 PM
  * @version : 1.0.0
  */
class ReplTest extends FlatSpec {

  "spark repl evaluate" should "run successful" in {
    val repl = new Repl(compilerOpts = List.empty[String])

    val result = repl.evaluate(
      """
        |import org.apache.spark._
        |import org.apache.spark.SparkContext._
        |import org.apache.spark.rdd._
        |
        |val dta:RDD[Int] = sparkContext.textFile("/var/log/syslog")
        |                               .map(_.size)
        |                               .distinct
      """.stripMargin)

    println(result)
  }
}
