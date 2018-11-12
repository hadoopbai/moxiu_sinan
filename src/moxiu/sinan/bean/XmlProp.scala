package moxiu.sinan.bean

import scala.xml.Elem
import scala.xml.XML

class XmlProp extends Serializable {

//  var jarName = ""
//  var confName: String = ""
//  var confPath = ""
//  var hDFSName = ""
//  var clusterUrl = ""
//  var xmlFile: Elem = null
//  def this(confName: String) = {
//    this
//    this.confName = confName
//    jarName = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath()
//    val fileseparator = System.getProperty("file.separator")
//    val jarpath = jarName.substring(0, jarName.lastIndexOf(fileseparator))
//    // 默认配置文件：/../conf/conf.xml
//    confPath = if (!confName.trim.equals("")) jarpath + "/../conf/" + confName else jarpath + "/../conf/conf.xml"
//    xmlFile = XML.load(confPath)
//  }
//
//  /**
//   * 解析基础属性
//   */
//  def getBaseProperties = {
//    val baseProperty = (xmlFile \ "BaseProperty")
//    hDFSName = (baseProperty \ "HDFSName").text
//    clusterUrl = (baseProperty \ "ClusterUrl").text
//    val bpb = new BasePropBean
//    bpb.setJarName(jarName)
//    bpb.setConfPath(confPath)
//    bpb.setHDFSName(hDFSName)
//    bpb.setClusterUrl(clusterUrl)
//    bpb
//  }
//
//  def printPorperties = {
//    getBaseProperties
//    Console println "================== 基础属性一览表 =================="
//    Console println "jar包路径:" + jarName
//    Console println "加载配置文件:" + confPath
//    Console println "HDFS集群名:" + hDFSName
//    Console println "HDFS URL:" + clusterUrl
//  }
}