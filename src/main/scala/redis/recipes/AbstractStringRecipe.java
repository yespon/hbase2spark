package redis.recipes;

import redis.RedisConnection;
import redis.RedisException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.utils.JedisUtil;

/**
 * @ClassName AbstractStringRecipe
 * <p>
 * </p>
 * @Author Yespon Liu(yespon@qq.com)
 * @Date 2018/10/26 12:10
 */
public class AbstractStringRecipe extends BaseRecipe {

    public String get(String key) throws RedisException {
        byte[] bytes = get(key.getBytes());
        if (bytes == null) {
            return null;
        } else {
            return new String(bytes);
        }
    }

    public byte[] get(byte[] key) throws RedisException {
        byte[] result = null;
        JedisPool pool = RedisConnection.getInstance().getPool();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            result = jedis.get(key);
        } catch (Exception e) {
            throw new RedisException(e.getMessage(), e);
        } finally {
            JedisUtil.close(jedis);
        }
        return result;
    }
}
