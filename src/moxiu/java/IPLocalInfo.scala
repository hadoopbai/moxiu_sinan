package moxiu.java

import scala.io.Source
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.util.TreeMap

object IPLocalInfo {

  val HDFSFileSytem: FileSystem = FileSystem.get(new Configuration)
  /**
   *  加载IP
   */
  def loadIpLib(iplibPath: String) = {
    val treeMap = new TreeMap[IpBean, Array[String]]
    val inputStream = HDFSFileSytem.open(new Path(iplibPath))
    // 加载ip列表
    val data = Source.fromInputStream(inputStream, "UTF-8").getLines
    data.foreach(line => {
      val lines = line.split("\t")
      val array = Array[String](lines(3), lines(4), lines(6))
      treeMap.put(new IpBean(lines(0), lines(1)), array)
    })
    treeMap
  }

  /**
   * 根据IP取省、市、运营商
   */
  def getLocal(ip: String, treeMap: TreeMap[IpBean, Array[String]]): Array[String] = {
    if ("".equals(ip)) Array("*", "*", "*") else {
      val formatIP = (for (ip <- ip.split("\\.")) yield { ("000".concat(ip)).takeRight(3) }).mkString(".")
      val array = treeMap.get(new IpBean(formatIP, formatIP))
      if (array == null || array.size == 0) Array("*", "*", "*") else array
    }
  }
}