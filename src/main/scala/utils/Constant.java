package utils;

/**
 * @ClassName Constant
 * <p>
 * </p>
 * @Author Yespon Liu(yespon@qq.com)
 * @Date 2018/10/29 10:38
 */
public class Constant {
    /**
     * SAPRK相关配置参数
     */
    public static final String EXCUTOR_CORES = "4";

    public static final String EXCUTOR_MEMORY = "8g";

    public static final String EXCUTOR_INSTANCES = "50";

    public static final String PARALLELISM = "500";

    public static final String SERIALIZER = "org.apache.spark.serializer.KryoSerializer";

    public static final String NETWORK_TIMEOUT_VALUE = "300";

    public static final String YARN_QUEUE = "alg_moji";

    /**
     * HBASE相关配置参数
     */
    public static final String ZOOKEPER = "hadoop050,hadoop057,hadoop064,hadoop071,hadoop078,hadoop084,hadoop088";

    public static final String HBASE_ROOTDIR = "hdfs://172.16.19.80:9000/hbase";

    public static final String HBASE_PORT = "2181";

    public static final String HBASE_TARGET_TABLE = "tags";

    public static final String DFS_SOCKET_TIMEOUT_VALUE = "180000";

    public static final String HBASE_RPC_TIMEOUT_VALUE = "18000000";


    /**
     *
     */
    public static final String COMMA_SPLIT = ",";

    public static final String TAB_SPLIT = "\t";
}
