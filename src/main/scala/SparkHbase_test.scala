package com.moji

import com.moji.redis.RedisConnection
import com.moji.redis.recipes.SimpleStorageRecipe
import com.moji.utils.Constant
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.HConstants.HBASE_RPC_TIMEOUT_KEY
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import redis.RedisConnection
import redis.recipes.SimpleStorageRecipe
import utils.Constant

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * 用户画像对外服务
  * <p>
  *     对用户画像数据进行处理，放入指定的Redis服务器中，以缓存方式对外提供服务。
  *
  *     当前用户画像存放在HBase中，但未处理互斥关系标签。
  *     Redis中，以用户uid为key值，value值为"platform_id，tagid，tagid，..."字符串的方式
  *     存储。
  * </p>
  *
  * @author Yespon Liu
  **/
class ProfileService {

  def ReadHBase(sparkSession: SparkSession): Unit = {
    val sc = sparkSession.sparkContext

    //配置hbase相关参数
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum", Constant.ZOOKEPER)
    hBaseConf.set("hbase.rootdir", Constant.HBASE_ROOTDIR)
    hBaseConf.set("hbase.zookeeper.property.clientPort", Constant.HBASE_PORT)
    hBaseConf.set("dfs.socket.timeout", Constant.DFS_SOCKET_TIMEOUT_VALUE)
    hBaseConf.set(TableInputFormat.INPUT_TABLE, Constant.HBASE_TARGET_TABLE)
    hBaseConf.set(HBASE_RPC_TIMEOUT_KEY, Constant.HBASE_RPC_TIMEOUT_VALUE)

    //读取数据
    val hBaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    hBaseRDD.cache()

    val resultRDD = hBaseRDD.map(_._2).flatMap(result => {
      val res = new ListBuffer[(String, String)]()
      val rowKey = Bytes.toString(result.getRow()).split("_")
      val tags = new StringBuffer()

      rowKey(0) match {
        case "and" => tags.append("1")
        case "ios" => tags.append("7")
      }

      val tagTimeList = new ListBuffer[(String, Long)]()
      for (_cell <- result.rawCells()) {
        //获取列名
        val qualifier: String = Bytes.toString(CellUtil.cloneQualifier(_cell))

        // 获取时间戳, 对于互斥tag，将选取其互斥组中最近更新时间的那一条记录
        val timestamp = _cell.getTimestamp
        tagTimeList.append((qualifier, timestamp))
      }

      //按时间对tagid排序(从大到小)
      val tagTime = tagTimeList.sortBy(_._2).reverse

      //保留tagid列表
      val tagList = tagTime.toMap.keys.toList

      //获取存在互斥条件的tagid
      val _tagList = tagList.filter(t => {
        t.startsWith("01") || t.startsWith("02") || t.startsWith("701") || t.startsWith("702")
      })

      if (_tagList != null) {
        val tagIdentifierTuple = new ListBuffer[(String, String)]()
        for (tag <- _tagList) {
          val identifier = tag.substring(0,5)
          tagIdentifierTuple.append((identifier, tag))
        }
        //根据tag父类标签做分组
        val tagTupleList = tagIdentifierTuple.groupBy(_._1)
        for (tag <- tagTupleList) {
          tags.append(Constant.COMMA_SPLIT)
          tags.append(tag._2(0))
        }
      }

      //将不存在互斥条件的tag放入结果串中
      for (tag <- tagList) {
        if (StringUtils.isNumeric(tag)
          && (!tag.startsWith("01") || !tag.startsWith("02")
          || !tag.startsWith("701") || !tag.startsWith("702"))) {
          tags.append(Constant.COMMA_SPLIT)
          tags.append(tag)
        }
      }

      res.append((rowKey(1), tags.toString()))
      res.toList
    })


    //对转换结果进行存入Redis操作
    val recipe = new SimpleStorageRecipe();
    resultRDD.repartition(1000).foreachPartition(rows => {
      val kvs = new ArrayBuffer[String]()
      rows.foreach(row => {
        kvs.append(row._1)
        kvs.append(row._2.toString)
      })
      recipe.mset(kvs.toArray:_*)
    })
    RedisConnection.getInstance().disconnect();
  }

  def main(args: Array[String]): Unit = {
    //配置spark参数
    val job_name = "profile_service"
    val sparkConf = new SparkConf().setAppName(job_name).setMaster("yarn")
      .set("spark.executor.cores", Constant.EXCUTOR_CORES)
      .set("spark.executor.memory", Constant.EXCUTOR_MEMORY)
      .set("spark.executor.instances", Constant.EXCUTOR_INSTANCES)
      .set("spark.default.parallelism", Constant.PARALLELISM)
      .set("spark.serializer", Constant.SERIALIZER)
      .set("spark.network.timeout", Constant.NETWORK_TIMEOUT_VALUE)
      .set("spark.yarn.queue", Constant.YARN_QUEUE)
    sparkConf.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))

    //创建spark context
    val spark = SparkSession.builder().config(conf = sparkConf)
      .enableHiveSupport()
      .getOrCreate()

  }
}
