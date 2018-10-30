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

    private JedisPool pool;

    public BaseRecipe(){}

    public BaseRecipe(JedisPool pool) {
        this.pool = pool;
    }

    public void del(String key) throws RedisException {
        del(key.getBytes());
    }

    public void del(byte[] key) throws RedisException {
        Jedis jedis;
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
