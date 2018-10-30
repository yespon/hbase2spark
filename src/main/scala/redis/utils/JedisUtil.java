package redis.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @ClassName JedisUtil
 * <p>
 * </p>
 * @Author Yespon Liu(yanpeng.liu@moji.com)
 */
public class JedisUtil {
    private final static Logger logger = LoggerFactory.getLogger(JedisUtil.class);

    /**
     *
     * @param jedis
     */
    public static void close(Jedis jedis) {
        try {
            jedis.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     *
     * @param pool
     */
    public static void close(JedisPool pool) {
        if (pool != null && !pool.isClosed()) {
            pool.close();
        }
    }

    /**
     *
     * @param bytes
     * @return
     */
    public static boolean isEmpty(byte[] bytes) {
        return bytes == null || bytes.length == 0;
    }

    /**
     *
     * @param str
     * @return
     */
    public static boolean isEmpty(String str) {
        return str == null || str.trim().length() == 0;
    }
}
