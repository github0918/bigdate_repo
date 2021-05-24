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
                   zkHost:String = "hadoop01",
                   zkPort:String = "2181",
                   topK:Int = 100,
                   htableName:String = "recommend:news-cf",
                   hfilePath:String = ""
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
        case "ItemCF" =>
          opt[String]('h', "zkhost").required().action((x, config) =>config.copy(zkHost=x)).text("zkhost not must bu null")
          opt[String]('p', "zkport").required().action((x, config) =>config.copy(zkPort=x)).text("zkport not must bu null")
          opt[Int]('k', "topK").required().action((x, config) =>config.copy(topK=x)).text("topK is not zero")
          opt[String]('t', "hbasetabe").required().action((x, config) =>config.copy(htableName=x)).text("htablename not must bu null")
          opt[String]('f', "hfilePath").required().action((x, config) =>config.copy(hfilePath=x)).text("hfilepath not must bu null")

        case "ALSCF" =>
          opt[String]('h', "zkhost").required().action((x, config) =>config.copy(zkHost=x)).text("zkhost not must bu null")
          opt[String]('p', "zkport").required().action((x, config) =>config.copy(zkPort=x)).text("zkport not must bu null")
          opt[Int]('k', "topK").required().action((x, config) =>config.copy(topK=x)).text("topK is not zero")
          opt[String]('t', "hbasetabe").required().action((x, config) =>config.copy(htableName=x)).text("htablename not must bu null")
          opt[String]('f', "hfilePath").required().action((x, config) =>config.copy(hfilePath=x)).text("hfilepath not must bu null")

        case "ItemBaseFeature" =>
        case "UserBaseFeature" =>
        case "ArticleEmbedding" =>
          opt[String]('h', "zkhost").required().action((x, config) =>config.copy(zkHost=x)).text("zkhost not must bu null")
          opt[String]('p', "zkport").required().action((x, config) =>config.copy(zkPort=x)).text("zkport not must bu null")
          opt[String]('t', "hbasetabe").required().action((x, config) =>config.copy(htableName=x)).text("htablename not must bu null")
          opt[String]('f', "hfilePath").required().action((x, config) =>config.copy(hfilePath=x)).text("hfilepath not must bu null")


        case "LRClassify" =>
        case "IrisLRClassify" =>

        case "UnionFeature" =>
          opt[String]('h', "zkhost").required().action((x, config) =>config.copy(zkHost=x)).text("zkhost not must bu null")
          opt[String]('p', "zkport").required().action((x, config) =>config.copy(zkPort=x)).text("zkport not must bu null")
          opt[String]('t', "hbasetabe").required().action((x, config) =>config.copy(htableName=x)).text("htablename not must bu null")
          opt[String]('f', "hfilePath").required().action((x, config) =>config.copy(hfilePath=x)).text("hfilepath not must bu null")


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
