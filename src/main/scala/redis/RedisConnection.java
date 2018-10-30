package redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.Properties;

/**
 * @ClassName RedisConnection
 * <p>
 * </p>
 * @Author Yespon Liu(yespon@qq.com)
 * @Date 2018/10/26 11:04
 */
public class RedisConnection {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private JedisPool pool;
//    private Jedis jedis;
    private JedisPoolConfig poolConfig = new JedisPoolConfig();

    private Properties properties = new Properties();
    private static final String REDIS_CONFIG = "redis.properties";
    private String host;
    private int port;
    private int timeout;
    private int database;
    private String password;

    private RedisConnection(){
        try {
            //初始化服务器连接信息
            properties.load(this.getClass().getClassLoader().getResourceAsStream(REDIS_CONFIG));
            host = properties.getProperty("redis.host");
            port = Integer.valueOf(properties.getProperty("redis.port"));
            timeout = Integer.valueOf(properties.getProperty("redis.timeout"));
            database = Integer.valueOf(properties.getProperty("redis.db"));
            password = properties.getProperty("redis.password");

            //初始化连接池配置信息
            poolConfig.setMaxTotal(Integer.valueOf(properties.getProperty("jedis.pool.maxActive")));
            poolConfig.setMaxIdle(Integer.valueOf(properties.getProperty("jedis.pool.maxIdle")));
            poolConfig.setMaxWaitMillis(Integer.valueOf(properties.getProperty("jedis.pool.maxWait")));
            poolConfig.setTestOnBorrow(Boolean.valueOf(properties.getProperty("jedis.pool.testOnBorrow")));
            poolConfig.setTestOnReturn(Boolean.valueOf(properties.getProperty("jedis.pool.testOnReturn")));
            poolConfig.setJmxEnabled(false);

            pool = new JedisPool(poolConfig, host, port, timeout, password, database);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
//            throw new RedisException(e.getMessage(), e);
        }
    }

    private static class Holder {
        final static RedisConnection instance = new RedisConnection();
    }

    public static RedisConnection getInstance() {
        return Holder.instance;
    }

//    public JedisPool connect() {
//        poolConfig.setJmxEnabled(false);
//        pool = new JedisPool(poolConfig, host, port, timeout, password, database);
//        return pool;
//    }

//    public void close() {
//        if (pool != null && !pool.isClosed()) {
//            pool.close();
//        }
//    }

    public void setPoolConfig(JedisPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public JedisPool getPool() {
        return pool;
    }

}
