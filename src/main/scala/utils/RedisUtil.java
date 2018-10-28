package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.Properties;

/**
 * @ClassName utils.RedisUtil
 * <p>
 * </p>
 * @Author Yespon Liu(yespon@qq.com)
 * @Date 2018/10/25 13:13
 */
public class RedisUtil {

    private static Logger logger = LoggerFactory.getLogger(RedisUtil.class.getClass());

    private static JedisPool jedisPool;

    private static String REDIS_CONFIG = "redis.properties";

    private RedisUtil() {}

    /**
     *
     */
    public static JedisPool initPool() {
        try {
            Properties properties = new Properties();
            //加载redis配置文件
            properties.load(RedisUtil.class.getClassLoader().getResourceAsStream(REDIS_CONFIG));
            //创建jedis池配置实例
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(Integer.valueOf(properties.getProperty("jedis.pool.maxActive")));
            poolConfig.setMaxIdle(Integer.valueOf(properties.getProperty("jedis.pool.maxIdle")));
            poolConfig.setMaxWaitMillis(Integer.valueOf(properties.getProperty("jedis.pool.maxWait")));
            poolConfig.setTestOnBorrow(Boolean.valueOf(properties.getProperty("jedis.pool.testOnBorrow")));
            poolConfig.setTestOnReturn(Boolean.valueOf(properties.getProperty("jedis.pool.testOnReturn")));

            //获取连接信息
            String host = properties.getProperty("redis.host");
            int port = Integer.valueOf(properties.getProperty("redis.port"));
            int timeout = Integer.valueOf(properties.getProperty("redis.timeout"));
            int db = Integer.valueOf(properties.getProperty("redis.db"));
            String password = properties.getProperty("redis.password");

            //实例化redis连接池
            jedisPool = new JedisPool(poolConfig, host, port, timeout, password, db);

        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);
        }
        return jedisPool;
    }

    /**
     *
     * @param host
     * @param port
     * @param timeout
     * @param password
     * @param db
     * @return
     */
    public static JedisPool initPool(String host, int port, int timeout, String password, int db) {
        try {
            Properties properties = new Properties();
            //加载redis配置文件
            properties.load(RedisUtil.class.getClassLoader().getResourceAsStream(REDIS_CONFIG));
            //创建jedis池配置实例
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(Integer.valueOf(properties.getProperty("jedis.pool.maxActive")));
            poolConfig.setMaxIdle(Integer.valueOf(properties.getProperty("jedis.pool.maxIdle")));
            poolConfig.setMaxWaitMillis(Integer.valueOf(properties.getProperty("jedis.pool.maxWait")));
            poolConfig.setTestOnBorrow(Boolean.valueOf(properties.getProperty("jedis.pool.testOnBorrow")));
            poolConfig.setTestOnReturn(Boolean.valueOf(properties.getProperty("jedis.pool.testOnReturn")));

            //实例化redis连接池
            jedisPool = new JedisPool(poolConfig, host, port, timeout, password, db);

        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);
        }
        return jedisPool;
    }
    /**
     *
     * @param jedisPool
     * @return
     */
    public static Jedis getConnection(JedisPool jedisPool) {
        //
        Jedis jedis = null;
        if (null != jedisPool) {
            jedis = jedisPool.getResource();
            logger.info("Get a redis client from connection pool.");
        } else {
            initPool();
            logger.info("The Jedis Pool is null, there will initial the redis connection pool.");
        }

        return jedis;
    }

    /**
     *
     * @param jedis
     */
    public static void closeConnection(Jedis jedis) {
        if (null != jedis) {
            jedis.close();
            logger.debug("The client connection is closed.");
        }
    }

    /**
     *
     * @param pool
     */
    public static void closePool(JedisPool pool) {
        if (null != pool) {
            pool.close();
            logger.debug("The client connection pool is closed.");
        }
    }

    // =============================================================
    // The operation of String for redis.
    //
    // =============================================================
    /**
     *
     * @param jedis
     * @param key
     * @param value
     * @param seconds
     */
    public static void setExpire(Jedis jedis, String key, String value, int seconds) {
        if (null != jedis) {
            jedis.setex(key, seconds, value);
        }
    }

    /**
     *
     * @param jedis
     * @param key
     * @param value
     */
    public static void set(Jedis jedis, String key, String value) {
        if (null != jedis) {
            jedis.set(key, value);
        }
    }

    /**
     *
     * @param jedis
     * @param key
     * @param value
     */
    public static void setnx(Jedis jedis, String key, String value) {
        if (null != jedis) {
            jedis.setnx(key, value);
        }
    }

    /**
     *
     * @param jedis
     * @param keysvalues
     */
    public static void mset(Jedis jedis, String... keysvalues) {
//        for (int i = 0; i < keysvalues.length; i++) {
//            System.out.println(keysvalues[i]);
//        }
        if (null != jedis) {
            jedis.mset(keysvalues);
        }
    }

    public static void mset(Jedis jedis, byte[] keysvalues) {
        jedis.mset(keysvalues);
    }
    /**
     *
     * @param jedis
     * @param keysvalues
     */
    public static void msetnx(Jedis jedis, String... keysvalues) {
        if (null != jedis) {
            jedis.msetnx(keysvalues);
        }
    }
}
