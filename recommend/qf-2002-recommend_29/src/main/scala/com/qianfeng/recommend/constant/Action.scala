package com.qianfeng.recommend.constant

/**
 * 事件枚举
 */
object Action extends Enumeration {
  type Action = Value

  val Click = Value("点击")  //0.1分
  val Share = Value("分享")   //0.15分
  val Comment = Value("评论")
  val Collect = Value("收藏")
  val Like = Value("点赞")

  def showAll = this.values.foreach(println)

  def withNameOpt(s: String): Option[Value] = values.find(_.toString == s)
}
