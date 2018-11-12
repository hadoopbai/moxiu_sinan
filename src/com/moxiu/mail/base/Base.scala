package com.moxiu.mail.base

import scala.collection.mutable.ArrayBuffer
import java.util.Vector

import scala.collection.mutable.Stack
import scala.collection.JavaConverters._
import java.util.regex.Pattern

object Base {

  def main(arg: Array[String]): Unit = {

    val str1 = """[{"城市":"北京","面积":16800,"人口":1600},{"城市":"上海","面积":6400,"人口":1800}]"""
    val str2 = """{ "animals": {  "dog": [ { "name": "Rufus[ss]","age":15 },{"name": "Marty","age": null} ], "dog1": [ { "name1": "Rufus1","age":15 },{"name1": "Marty1","age": null} ] }} """
    //    , "dog1": [ { "name1": "Rufu1","age1":15 },{"name1": "Marty","age1": null} ]
    println(str2)
    val jsonObj = new JsonAnalysis(str2)
    jsonObj.init
    jsonObj.showId
    val IndexMap = jsonObj.jsonIndexMap
    val map = Array(
      "p1" -> "1-1-2-2:map:@age",
      "p2" -> "1-1-1-2:map:@name1",
      "p3" -> "1-1-1-1:map:@age",
      "p4" -> "1-1-1-1:map:@name1",
      "p5" -> "1-1-1-2",
      "p6" -> "1-1-2-1:map:@age",
      "dogList" -> "1-1-1",
      "dog" -> "1-1-2-1").map(f => (f._1, if (IndexMap.get(f._2) == None) "" else IndexMap.get(f._2).get)).toMap

    
    val moderStr =s"""{ "animals": {  "dog": ${map("p4")}, "dog1": ${map("p5") }} """
    println(moderStr)
//   val ss  =  StringContext(moderStr,"").s("""${map("p4")},${map("p5")}""")
//  println(ss)
    
//val ss  = StringContext("Hello ${map}", "aaaa", "bbb").s(map("p4"),map("p5") )


  }

}

class JsonAnalysis(json: String) {
  val optStack = Stack[(Int, (Char, Int), Int)]()
  val optStackList = Stack[(String, ((Char, Int), (Char, Int)))]()
  var jsonNodeInfoList = List[(String, String, Int, Int, String)]()
  var jsonInfoWithIndexMap = Map[String, (String, String, Int, Int, String)]()
  var jsonIndexMap = Map[String, String]()
  var jsonIdList = List[String]()
  var endNodeList = List[String]()
  var jsonCharList = Array[Char]()

  /**
   * 解析用户自定义模版，找出变量所在位置
   */
  def analysisDoc(jsonMode: String) = {
    val charArray = jsonMode.toCharArray()

    (for (index <- 0 until charArray.size) {
      //      Console println charArray(index)
      if ("$" == charArray(index)) Console println charArray(index)
    })
  }

  def showId = jsonIndexMap.map(f => Console println f)
  /**
   * 设定原始json
   * 1、获取各结点的依赖路径
   * 2、获取终结点的路径地址
   */
  def init = {
    getOptStack(json)
    getEndNode
    getNodeValue
  }

  /**
   * 获取每个结点的依赖路径
   */
  def getOptStack(str: String) = {
    val text = str.trim
    jsonCharList = text.toCharArray
    var strOptCount = 0
    for (index <- 0 until jsonCharList.size) {
      val char = jsonCharList(index)
      // 不解析“”的内容
      if (char == '"') strOptCount += 1
      if (strOptCount % 2 == 0) {
        char match {
          case '{' => getMatch(char, index, '}')
          case '}' => getMatch(char, index, '{')
          case '[' => getMatch(char, index, ']')
          case ']' => getMatch(char, index, '[')
          case _   =>
        }
      }
    }
    jsonNodeInfoList = optStackList.map(opt => {
      val opt_char = opt._2._1._1.toString + opt._2._2._1.toString
      val startIndex = opt._2._1._2
      val endIndex = opt._2._2._2
      val content = text.substring(startIndex, endIndex + 1)
      (opt._1, opt_char, startIndex, endIndex, content)
    }).toList
    // 对json结点连表排序
    jsonIdList = jsonNodeInfoList.map(_._1).sorted
    jsonInfoWithIndexMap = (jsonIdList.zip(jsonNodeInfoList)).toMap
    // (id,json内容)
    jsonIndexMap = jsonInfoWithIndexMap.map(line => (line._1, line._2._5))
    println("---------获取每个结点的依赖路径------------")
    jsonNodeInfoList foreach println
  }

  /**
   * 获取终结点的路径地址
   */
  def getEndNode = {
    // 最终结点
    endNodeList = (for (i <- 0 until jsonIdList.size) yield {
      var flg = true
      for (j <- i + 1 until jsonIdList.size) {
        if (jsonIdList(j).startsWith(jsonIdList(i))) flg = false
      }
      (flg, jsonIdList(i))
    }).filter(_._1).map(_._2).toList
    // TODO
    println("---------获取终结点的路径地址------------")
//    endNodeList foreach println
  }

  def getNodeValue = {
    endNodeList.map(id => {
      //   (id, opt_char, startIndex, endIndex, content)
      val json = jsonInfoWithIndexMap(id)
      val opt_char = json._2
      val contentwithopt = json._5.trim
      val contents = contentwithopt.substring(1, contentwithopt.length - 1).trim
      val subJsonCharList = contents.toCharArray
      var strOptCount = 0
      val optArray = ArrayBuffer[(Char, Int)]()
      optArray += ((',', -1))
      for (index <- 0 until contents.size) {
        val char = subJsonCharList(index)
        // 不解析“”的内容
        if (char == '"') strOptCount += 1
        if (strOptCount % 2 == 0) {
          char match {
            //            case ':' => optArray += ((char, index))
            case ',' => optArray += ((char, index))
            case _   =>
          }
        }
      }
      optArray += ((',', contents.size))
      val commaArray = optArray.filter(opt => opt._1 == ',').map(_._2).sliding(2).toList
      if (opt_char == "{}") {
        commaArray.map(index => {
          val mapValue = contents.substring(index(0) + 1, index(1)).split(":")
          val keys = mapValue(0).trim
          val key = keys.substring(1, keys.length - 1).trim
          val value = mapValue(1)
          id + ":map:@" + key
          jsonIndexMap += ((id + ":map:@" + key -> value))
        })
      } else {
        // []
        for (index <- 0 until commaArray.size) {
          val start = commaArray(index)(0)
          val end = commaArray(index)(1)
          val contentText = contents.substring(start, end)
          jsonIndexMap += ((id + ":array[" + index + "]" -> contentText))
        }
      }
    })

  }

  /**
   * char：json出现的操作字符
   * index：char出现的位置
   * matchChar： 跟char匹配的另一半字符
   */
  def getMatch(char: Char, index: Int, matchChar: Char) {

    // 进栈
    if (char == '{' || char == '[') {
      // 取父结点的层数
      val layIndex = if (!optStack.isEmpty) optStack.head._1 else 0
      // 父结点中记录的子结点的个数（同层下有多少个子结点）
      val chindIndex = if (!optStack.isEmpty) optStack.head._3 else 0

      if (!optStack.isEmpty) {
        // 修改父结点中记录的子结点数加1
        val tmp = optStack.pop
        optStack.push((tmp._1, tmp._2, tmp._3 + 1))
        // 追加新结点，初始子结点数为0
        optStack.push((tmp._3 + 1, (char, index), 0))
        // 父结点层数从1记起，初始子结点数为0
      } else optStack.push((1, (char, index), 0))

    } else if (char == '}' || char == ']') {
      // 可能出栈
      val top_leftOpt = optStack.head._2._1
      if (top_leftOpt == matchChar) {
        // 出栈
        val top_left = optStack.pop
        val childIndex = if (optStack.size > 0) optStack.head._3 else ""

        val nodeLink_headIndex = if (optStack.size > 0) optStack.reverse.map(f => (f._1).toString).mkString("-") else "1"
        val nodeLink = if (childIndex != "") nodeLink_headIndex + "-" + childIndex else nodeLink_headIndex
        optStackList.push((nodeLink, (top_left._2, (char, index))))
      } else {
        println("输入的Json 串格式有误。")
      }
    } else {
      println("输入的Json 串格式有误。")
    }

  }
}