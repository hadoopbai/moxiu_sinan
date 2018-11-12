package com.moxiu.mail.base

object tttt {
  def main(arg: Array[String]): Unit = {
    val list1 = Array("a", "b", "c", "d")
    val t2 = new t2(list1)
    // 匿名函数：(f: String) => f + "_Moxiu"
    val result = t2.testMap(f => f + "_Moxiu")
    result foreach println

    def flter(str: String) = if (str.length() > 1) true else false

    val result1 = t2.testFilter(flter)
    result1 foreach println

  }
}

trait t1 {
  var list = Array[String]()
  var count =0
  def fun(msg: String) // 抽象方法
  def testMap(f: String => String) = for (e <- list) yield f(e)
  def testFilter(f: String => Boolean) = for (e <- list) yield f(e)
}

class t2(list1: Array[String]) extends t1 {
  list = list1
  def fun(msg: String) = { println(msg) }
}