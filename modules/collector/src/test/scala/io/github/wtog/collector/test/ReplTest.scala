package io.github.wtog.collector.test

import io.github.wtog.collector.Repl
import org.scalatest.FlatSpec

/**
  * @author : tong.wang
  * @since : 10/6/18 9:38 AM
  * @version : 1.0.0
  */
class ReplTest extends FlatSpec {

  "collector repl evaluate" should "run successful" in {
    val repl = new Repl(compilerOpts = List.empty[String])

    val result = repl.evaluate(
      """
        |import io.github.wtog.processor.{Page, PageProcessor, RequestHeaders}
        |import io.github.wtog.spider.{Spider, SpiderPool}
        |
        |final case class BaiduPageProcessor() extends PageProcessor {
        |
        |  override def process(page: Page): Unit = {
        |    val document = page.jsoupParser
        |
        |    page.addPageResultItem(Map("title" -> document.title()))
        |
        |    page.addTargetRequest("http://www.baidu.com")
        |  }
        |
        |  override def requestHeaders: RequestHeaders = {
        |    RequestHeaders(
        |      domain = "www.baidu.com",
        |      headers = Some(Map("Content-Type" -> "text/html; charset=GB2312")), useProxy = true)
        |  }
        |
        |  override def targetUrls: List[String] = {
        |    List("http://www.baidu.com")
        |  }
        |}
        |
        |Spider(pageProcessor = BaiduPageProcessor()).start()
        |SpiderPool.fetchAllSpiders()
      """.stripMargin)

    println(result)
  }
}
