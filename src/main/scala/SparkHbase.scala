import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.HConstants.HBASE_RPC_TIMEOUT_KEY
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis
import utils.RedisUtil

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @author Yespon Liu(yespon@qq.com)
  **/
object SparkHbase {

  def main(args: Array[String]): Unit = {
    // 创建spark配置
    val job_name = ""
    val sparkConf = new SparkConf().setAppName(job_name).setMaster("yarn")
      .set("spark.executor.cores", "4").set("spark.executor.memory", "8g")
      .set("spark.executor.instances", "50")
      .set("spark.default.parallelism", "500")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.network.timeout", "300").set("spark.yarn.queue", "alg_moji")
    /* 如果你要序列化的对象比较大，可以增加参数spark.kryoserializer.buffer所设置的值。
     * 如果你没有注册需要序列化的class，Kyro依然可以照常工作，但会存储每个对象的全类名(full class name)，
     * 这样的使用方式往往比默认的 Java serialization 还要浪费更多的空间。
     **/
    sparkConf.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))

    // 创建spark context
    val spark = SparkSession.builder().config(conf = sparkConf).getOrCreate()
    val sc = spark.sparkContext

    // scan + scan-filter
//    val scan = new Scan()
//    scan.setCacheBlocks(false)
//    scan.setCaching(1000)
//
//    val proto = ProtobufUtil.toScan(scan)
//    val Scan2String = Base64.encodeBytes(proto.toByteArray())

    // 创建HBase配置
    val hBaseConf = HBaseConfiguration.create()
    //设置读取的表
    hBaseConf.set("hbase.zookeeper.quorum","hadoop050,hadoop057,hadoop064,hadoop071,hadoop078,hadoop084,hadoop088")
    hBaseConf.set("hbase.rootdir","hdfs://172.16.19.80:9000/hbase")
    hBaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hBaseConf.set("dfs.socket.timeout","180000")
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "tags")
    hBaseConf.set(HBASE_RPC_TIMEOUT_KEY, "18000000")
//    hBaseConf.set(TableInputFormat.SCAN, Scan2String)

    // 全量读取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

//    val cnt = hbaseRDD.count()
//    println("========"+cnt+"========")

    hbaseRDD.cache()


    hbaseRDD.take(4).foreach(kv => println(kv))

    val rowKeyRDD = hbaseRDD.map(tuple => tuple._1).map(item => Bytes.toString(item.get()))
    rowKeyRDD.take(5).foreach(println)

    val resultRDD = hbaseRDD.map(tuple => tuple._2)
//    resultRDD.count()
    resultRDD.take(3).foreach(v => println(v))

    println("-----------")
    val datas = resultRDD.flatMap(result => {
//      val res = new ListBuffer[(String, List[String])]()
      val res = new ListBuffer[(String, String)]()
      val rowkey: String = Bytes.toString(result.getRow)
//      val tagList = new ArrayBuffer[String]()

      val tags = new StringBuffer()
      val rowkeys = rowkey.split("_")
      rowkeys(0) match {
        case "and" => tags.append("1")
        case "ios" => tags.append("7")
      }

      val SPLITER = ","

      for (_cell <- result.rawCells) {
        val family:String = Bytes.toString(CellUtil.cloneFamily(_cell))
        val qualifier: String = Bytes.toString(CellUtil.cloneQualifier(_cell))
//        tagList.append(qualifier)
        if (StringUtils.isNumeric(qualifier)) {
          tags.append(SPLITER)
          tags.append(qualifier)
        }
      }
//      res.append((rowkey, tagList.toList))
      res.append((rowkeys(1), tags.toString()))
      res.toList
    })

    datas.take(10).foreach(tuple => println(tuple))
    println("------------")

//    val rowkeyValueRDD = hbaseRDD.map(tuple => Tuple2(new String(tuple._1), new String(tuple._2)))
//    rowkeyValueRDD.take(4).foreach(kv => println(kv))

    val keyValueRDD = resultRDD.map(result => (Bytes.toString(result.getRow()).split(" ")(0), Bytes.toString(result.value)))
    keyValueRDD.take(5).foreach(kv => println(kv))




    //    val jedis: Jedis = utils.RedisUtil.getJedisConn("r-2ze5b49d36bd30a4.redis.rds.aliyuncs.com",6379,"xg28KbjA",10000,9)
//    val jedis: Jedis = RedisUtil.getConnection(RedisUtil.initPool())
//    val jedis: Jedis = RedisUtil.getConnection(RedisUtil.initPool("172.16.21.28",6379,10000,"123456",9))

    val start = System.currentTimeMillis()
    val kvs = new ArrayBuffer[String]()

//    datas.take(10000).foreach(tup => {
//      kvs.append(tup._1)
////      println(tup._1)
//      kvs.append(tup._2.toString)
////      println(tup._2)
////      utils.RedisUtil.set(jedis, tup._1, tup._2.toString())
//    })

    //TODO:对数据分组进行入库的两种思路 1：uid求余 2：重新分区（100~1000）
    //TODO: 3//未验证
//    datas.groupBy(data => {Integer.getInteger(data._1) % 1000}).mapValues(values => {
//      values.foreach(tup =>{
//        kvs.append(tup._1)
//        kvs.append(tup._2.toString)
//      })
//      RedisUtil.mset(jedis, kvs.toArray:_*)
//    })
    //

//    def f(iterator: Iterator): Unit = {
//      val kvs = new ArrayBuffer[String]()
//      for (x <- iterator) {
//        kvs.append(x(0))
//        kvs.append(x(1).toString)
//      }
//      RedisUtil.mset(jedis, kvs.toArray:_*)
//    }

    //TODO:2
    datas.repartition(1000).foreachPartition(rows => {
      val jedis: Jedis = RedisUtil.getConnection(RedisUtil.initPool("172.16.21.28",6379,10000,"123456",9))
      val kvs = new ArrayBuffer[String]()
      rows.foreach(row => {
        kvs.append(row._1)
        kvs.append(row._2.toString)
      })
      RedisUtil.mset(jedis, kvs.toArray:_*)
      RedisUtil.closeConnection(jedis)
    })

    //TODO:1
//    datas.repartition(1000).foreachPartition(rows => {
//      val jedis: Jedis = RedisUtil.getConnection(RedisUtil.initPool("172.16.21.28",6379,10000,"123456",9))
//      val pipe = jedis.pipelined
//
//      rows.foreach(r => {
//        val redisKey = r._1
//        val redisValue = r._2
//        pipe.set(redisKey, redisValue)
//      })
//
//      pipe.sync()
//      RedisUtil.closeConnection(jedis)
//    })



//    RedisUtil.mset(jedis, kvs.toArray:_*)
//    println(kvs.toString)
//    RedisUtil.closeConnection(jedis)
    val end = System.currentTimeMillis()
    println("Total time is:" + (end - start) + "ms")

    spark.stop()
  }



}
