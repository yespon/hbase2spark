package redis.recipes;

import redis.RedisConnection;
import redis.RedisException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @ClassName BaseRecipe
 * <p>
 * </p>
 * @Author Yespon Liu(yespon@qq.com)
 * @Date 2018/10/26 12:10
 */
public abstract class BaseRecipe {

    public void del(String key) throws RedisException {
        del(key.getBytes());
    }

    public void del(byte[] key) throws RedisException {
        JedisPool pool = RedisConnection.getInstance().getPool();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.del(key);
        } catch (Exception e) {
            throw new RedisException(e);
        } finally {
            pool.close();
        }
    }
}
