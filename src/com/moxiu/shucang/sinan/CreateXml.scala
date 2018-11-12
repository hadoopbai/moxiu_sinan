package com.moxiu.shucang.sinan

import scala.io.Source
import java.io.{ PrintWriter, File }
import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
object CreateXml {

  def main(args: Array[String]): Unit = {
    val date_1 = args(0)
    val date_2 = args(1)
    val confPath = getpath
    val contentsList = getFileLines(confPath+"conf.xml_templet")
    val linespearator = System.getProperty("line.separator")
    val xmlText = contentsList.mkString(linespearator)
    
    val p = Pattern.compile("<member>([\\s\\S]*?)</member>")
    val matcher = p.matcher(xmlText)
    var frist_match_flg = true
    var headInfo = ""
    var endindex = 0
    val productList = ArrayBuffer[(String, Map[String, String])]()
    while (matcher.find) {
      // 取头信息
      if (frist_match_flg) {
        frist_match_flg = false
        headInfo = xmlText.substring(0, matcher.start(0))
      }
      // 取尾信息index
      endindex = matcher.end(0)
      // 产品info
      val infoStr = matcher.group(0)
      // 解析产品信息
      val map = Map[String, String]()
      val p_productName = Pattern.compile("<productName>([\\s\\S]*?)</productName>")
      val p_productId = Pattern.compile("<productId>([\\s\\S]*?)</productId>")
      val p_xmlName = Pattern.compile("<xmlName>([\\s\\S]*?)</xmlName>")
      val m_productName = p_productName.matcher(infoStr)
      val m_productId = p_productId.matcher(infoStr)
      val m_xmlName = p_xmlName.matcher(infoStr)
      val productName = if (m_productName.find) m_productName.group(1) else ""
      val productId = if (m_productId.find) m_productId.group(1) else ""
      val xmlName = if (m_xmlName.find) m_xmlName.group(1) else ""
      map += ("productName" -> productName)
      map += ("productId" -> productId)
      map += ("xmlName" -> xmlName)
      productList += ((infoStr, map))
    }
    // 取尾信息
    val tailInfo = xmlText.substring(endindex)
    productList.foreach(p => {
      val productStr = p._1
      val productMap = p._2
      val productName = productMap.get("productName").get.trim
      val productId = productMap.get("productId").get.trim
      val xmlName = productMap.get("xmlName").get.trim
      if (productName != "" && productId != "" && xmlName != "") {

        val tail = tailInfo.replaceAll("\\{productId\\}", productId)
          .replaceAll("\\{productName\\}", productName)
          .replaceAll("\\{productName\\}", productName)
          .replaceAll("\\{date\\}", date_1)
          .replaceAll("\\{date_2\\}", date_2)

        // conf文件路径
        val confFile = new File(confPath + xmlName)
        // 如果conf.xml 存在则删除
        if (confFile.exists) confFile.delete

        val writer = new PrintWriter(confFile, "UTF-8")
        writer.write(headInfo + linespearator)
        writer.write(productStr + linespearator)
        writer.write(tail)
        writer.flush
        writer.close
      }else Console println "请检查配置文件【conf/conf.xml_templet】里的【productName】、【productId】、【xmlName】结点。"
    })
  }

  def getpath = {
    val jarName = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    val fileseparator = System.getProperty("file.separator")
    val jarpath = jarName.substring(0, jarName.lastIndexOf(fileseparator))
    // 魔板配置文件：/../conf/conf.xml_templet
    jarpath + "/../conf/"
  }
  def getFileLines(fileName: String) = Source.fromFile(fileName, "UTF-8").getLines.toList
}