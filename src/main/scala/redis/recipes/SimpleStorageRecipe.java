package redis.recipes;

import redis.RedisConnection;
import redis.RedisException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.utils.JedisUtil;

/**
 * @ClassName SimpleStorageRecipe
 * <p>
 * </p>
 * @Author Yespon Liu(yespon@qq.com)
 * @Date 2018/10/26 12:25
 */
public class SimpleStorageRecipe extends AbstractStringRecipe {

    private JedisPool pool;

    public SimpleStorageRecipe(){}

    public SimpleStorageRecipe(JedisPool pool) {
        this.pool = pool;
    }

    public void set(String key, String data) throws RedisException {
        set(key.getBytes(), data.getBytes());
    }

    public void set(byte[] key, byte[] data) throws RedisException {

        Jedis jedis = null;
        try {
            jedis = this.pool.getResource();
            jedis.set(key, data);
        } catch (Exception e) {
            throw new RedisException(e.getMessage(), e);
        } finally {
            JedisUtil.close(jedis);
        }
    }

    public void mset(String... keysvalues) throws RedisException {

        Jedis jedis = null;
        try {
            jedis = this.pool.getResource();
            jedis.mset(keysvalues);
        } catch (Exception e) {
            throw new RedisException(e.getMessage(), e);
        } finally {
            JedisUtil.close(jedis);
        }
    }

}
