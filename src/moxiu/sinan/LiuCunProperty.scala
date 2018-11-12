package moxiu.sinan

class LiuCunProperty(confName: String) extends BaseProperty(confName) {

  def getProperties = {
    // 留存
    val product = (xmlFile \ "product")
    val liuCun = (product \ "LiuCun")
    val input = (liuCun \ "Input").text
    val minFieldSize = (liuCun \ "MinFieldSize").text.toInt
    val output = (liuCun \ "Output").text
    // 合并留存
    val liuCunMerge = (product \ "LiuCunMerge")
    val mergeinput = (liuCunMerge \ "Input").text
    val mergeoutput = (liuCunMerge \ "Output").text
    // 日表抽出
    val dayExtract = (product \ "DayExtract")
    val dayinput = (dayExtract \ "Input").text
    val dayoutput = (dayExtract \ "Output").text
    val fieldSize = (dayExtract \ "FieldSize").text.toInt

    val bean = new LiuCunBean
    getBaseProperties(bean)

    bean.setLc_input(input)
    bean.setLc_minFieldSize(minFieldSize)
    bean.setLc_output(output)

    bean.setMergeinput(mergeinput)
    bean.setMergeoutput(mergeoutput)

    bean.setDayinput(dayinput)
    bean.setDayoutput(dayoutput)
    bean.setFieldSize(fieldSize)
    bean
  }
}