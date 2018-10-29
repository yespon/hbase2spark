package redis.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/**
 * @ClassName JedisUtil
 * <p>
 * </p>
 * @Author Yespon Liu(yanpeng.liu@moji.com)
 */
public class JedisUtil {
    private final static Logger logger = LoggerFactory.getLogger(JedisUtil.class);

    public static void close(Jedis jedis) {
        try {
            jedis.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static boolean isEmpty(byte[] bytes) {
        return bytes == null || bytes.length == 0;
    }

    public static boolean isEmpty(String str) {
        return str == null || str.trim().length() == 0;
    }
}
