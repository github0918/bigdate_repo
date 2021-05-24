package com.qianfeng.recommend.config

import scopt.OptionParser

/**
 * 参数解析工具类
 *
 * @param env  运行时设置的环境 ，， dev/pro/test
 * @param checkpointDir   设置job运行时的检测点目录
 */
case class Config(
                   env: String = "",
                   checkpointDir: String ="",
                   username:String = "",
                   password:String = "",
                   url:String = "",
                   topK:Int = 25,
                   startDate:String="",
                   endDate:String=""
                 )


//config的对象
object Config {

  //解析参数列表
  def parseConfig(obj: Object, args: Array[String]): Config = {
    //正则\$ 替换成"";;;获取类名称
    val programName = obj.getClass.getSimpleName.replaceAll("\\$", "")
    //获取scopt对象
    val parser: OptionParser[Config] = new scopt.OptionParser[Config]("spark ss " + programName) {
      head(programName, "1.0") //应用程序版本，，可以不写
      //.required()是必须要传的参数  .action() :添加回调函数,传递值-将x(x是参数列表中对应的值)值copy给env  .text()： 添加该参数的使用描述
      opt[String]('e', "env").required().action((x, config) =>config.copy(env=x)).text("env: dev or prod")
      //使用程序名称进行匹配，然后赋予更多的参数
      programName match {
        case "LabelGenerator" =>
          opt[String]('n', "username").required().action((x, config) =>config.copy(username=x)).text("username must bu null")
          opt[String]('p', "password").required().action((x, config) =>config.copy(password=x)).text("password must bu null")
          opt[String]('u', "url").required().action((x, config) =>config.copy(url=x)).text("url must bu null")

        case "NewsContentSegment" =>

        case "NewsKeyWords" =>
          opt[Int]('t', "topk").required().action((x, config) =>config.copy(topK=x)).text("topk must bu null")

        case "NewsWord2Vec" =>

        case "UserEmbedding" =>
          opt[String]('s', "startDate").required().action((x, config) =>config.copy(startDate=x)).text("startdata must bu not null")
          opt[String]('d', "endDate").required().action((x, config) =>config.copy(endDate=x)).text("enddate must bu not null")

        case _ =>
      }
    }

    //解析
    val myConfig: Option[Config] = parser.parse(args, Config())

    //取对应的值
    myConfig match {
      case Some(conf) => conf
      case None => {
        // println("cannot parse args")
        System.exit(-1)
        null
      }
    }

  }
}
