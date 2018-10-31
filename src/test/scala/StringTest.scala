
/**
  * @ClassName StringTest
  *            <p>
  *            </p>
  * @Author Yespon Liu(yespon@qq.com)
  * @Date 2018/10/31 13:04
  */
object StringTest {
  def main(args: Array[String]): Unit = {
    //标签串（共134+2个）
    val tags = "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
    println(tags.length)

    val test = new StringBuffer(tags)
    println(test.toString)
    test.setCharAt(2, '1')
    println(test)
  }

}
