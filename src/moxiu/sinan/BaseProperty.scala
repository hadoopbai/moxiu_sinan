package moxiu.sinan

import scala.xml.XML
import scala.xml.Elem

/**
 * @author 宿荣全
 * @data 2017.1.6
 * 解析配置文件中的基本属性
 */
class BaseProperty extends Serializable {

  var jarName = ""
  var confName: String = ""
  var confPath = ""
  var xmlFile: Elem = null
  def this(confName: String) = {
    this
    this.confName = confName
    jarName = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    val fileseparator = System.getProperty("file.separator")
    val jarpath = jarName.substring(0, jarName.lastIndexOf(fileseparator))
    // 默认配置文件：/../conf/conf.xml
    confPath = jarpath + "/../conf/" + confName
    xmlFile = XML.load(confPath)
  }

  /**
   * 解析基础属性
   */
  def getBaseProperties(bpb: BasePropBean) = {
    val Base = (xmlFile \ "Base")
    val ipFile = (Base \ "IpFile").text
    val typeId = (Base \ "productList" \ "member" \ "typeId").text.toInt
    bpb.setIpFile(ipFile)
    bpb.setTypeId(typeId)
  }
}