package moxiu.sinan

import scala.beans.BeanProperty
class LiuCunBean extends BasePropBean {
  // 桌面留存
  @BeanProperty var lc_input = ""
  @BeanProperty var lc_minFieldSize = 0
  @BeanProperty var lc_output = ""
  // 桌面合并留存
  @BeanProperty var mergeinput = ""
  @BeanProperty var mergeoutput = ""
  // 桌面日表抽出
  @BeanProperty var dayinput = ""
  @BeanProperty var dayoutput = ""
  @BeanProperty var fieldSize = 0
}