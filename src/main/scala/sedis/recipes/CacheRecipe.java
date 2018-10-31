package sedis.recipes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sedis.RedisException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import sedis.utils.JedisUtil;

import java.util.Date;

/**
 * @ClassName CacheRecipe
 * <p>
 * </p>
 * @Author Yespon Liu(yespon@qq.com)
 * @Date 2018/10/26 12:24
 */
public class CacheRecipe extends AbstractStringRecipe {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private JedisPool pool;

    public CacheRecipe(){}

    public CacheRecipe(JedisPool pool) {
        this.pool = pool;
    }

    /**
     *
     * @param key
     * @param expire
     * @param data
     * @throws RedisException
     */
    public void put(String key, int expire, String data) throws RedisException {
        put(key.getBytes(), expire, data.getBytes());
    }

    /**
     *
     * @param key
     * @param expireAt
     * @param data
     * @throws RedisException
     */
    public void put(String key, Date expireAt, String data) throws RedisException {
        put(key.getBytes(), expireAt, data.getBytes());
    }

    /**
     *
     * @param key
     * @param expire
     * @param data
     * @throws RedisException
     */
    public void put(byte[] key, int expire, byte[] data) throws RedisException {
        Jedis jedis = null;
        try {
            jedis = this.pool.getResource();
            redis.clients.jedis.Transaction tx = jedis.multi();
            tx.set(key, data);
            tx.expire(key, expire);
            tx.exec();
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RedisException(e.getMessage(), e);
        } finally {
            logger.debug("Redis connection is closed.");
            JedisUtil.close(jedis);
        }
    }

    /**
     *
     * @param key
     * @param expireAt
     * @param data
     * @throws RedisException
     */
    public void put(byte[] key, Date expireAt, byte[] data) throws RedisException {
        Jedis jedis = null;
        try {
            jedis = this.pool.getResource();
            Transaction tx = jedis.multi();
            tx.set(key, data);
            tx.expireAt(key, expireAt.getTime());
            tx.exec();
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RedisException(e.getMessage(), e);
        } finally {
            JedisUtil.close(jedis);
        }
    }
}
