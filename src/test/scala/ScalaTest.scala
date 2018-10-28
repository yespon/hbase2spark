
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import utils.RedisUtil

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @author Yespon Liu(yespon@qq.com)
  **/
object ScalaTest {

  def main(args: Array[String]): Unit = {
    val res = new ArrayBuffer[(String, String)]()
    val res1 = new ArrayBuffer[String]()
    val res2 = new StringBuffer()

    val rowkey = "111111111"
    val tagList = new ArrayBuffer[String]()
    val tags = new StringBuffer()
    val SPLITER = ","
    tagList.append("2222222")
    tags.append("2222222")
    tagList.append("4444444")
    tags.append(SPLITER + "4444444")
    tagList.append("6666666")
    tags.append(SPLITER + "6666666")
    println(tagList.toList)
    println(tags.toString)

    res.append((rowkey, tagList.toList.toString()))

    val tagList1 = new ArrayBuffer[String]()
    val tags1 = new StringBuffer()


    tagList1.append("3333333")
    tags1.append("3333333")
    tagList1.append("5555555")
    tags1.append(SPLITER + "5555555")
    tagList1.append("7777777")
    tags1.append(SPLITER + "7777777")
    println(tagList.toList)
    println(tags1.toString)

    res.append(("1010101010", tagList.toList.toString()))
    println(res.mkString(",").toString())

    res1.append("1010100000")
    res1.append(tags.toString)
    res1.append("121212121212")
    res1.append(tags1.toString)
    println(res1)
    println(res1.length)

    res2.append("1010100000")
    res2.append(SPLITER)
    res2.append(tags.toString)
    res2.append(SPLITER)
    res2.append("121212121212")
    res2.append(SPLITER)
    res2.append(tags1.toString)
    println(res2)
    println(res2.length)

//    val config: JedisPoolConfig = new JedisPoolConfig()
//    val pool: JedisPool = new JedisPool(config, "172.16.21.28",6379,"",10000,9)
    val jedis: Jedis = RedisUtil.getConnection(RedisUtil.initPool("172.16.21.28",6379,10000,"123456",9))

    val kvs = new StringBuffer()
//    if (kvs==null || kvs.length() == 0)
//      println("------------null")
    res.foreach(tup => {
      if (kvs.length() != 0) {
        kvs.append(",")
      }
      kvs.append(tup._1)
      println(tup._1)
      kvs.append(tup._2.toString)
      println(tup._2)
//      utils.RedisUtil.set(jedis, tup._1, tup._2.toString())
    })

    println(kvs.toString)

//    val ss = new Array[String](res1.length)
//    val ss = scala.collection.JavaConversions.bufferAsJavaList(res1)


    var mm = Map[String,String]()
    mm += ("key1" -> "v1", "k2"->"v2")

    res.foreach(tup => {
      mm.+= (tup._1 -> tup._2.toString)
    })
    println(mm)

    val key: String = "and_22324456"
    println(key.split("_")(0))
    key.split("_")(0) match {
      case "and" => println("1")
      case "ios" => println("7")
    }
    println(key.split("_")(1))
    for (s <- key.split("_")){
      println(s.toString)
    }

    RedisUtil.mset(jedis, res1.toArray:_*)

//    utils.RedisUtil.closeConnection(jedis)
  }
}
