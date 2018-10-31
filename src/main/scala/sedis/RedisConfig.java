package sedis;

import com.moji.utils.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * RedisConfig
 * <p>
 * </p>
 * @author Yespon Liu
 */
public class RedisConfig {
    private static final Logger logger = LoggerFactory.getLogger(RedisConfig.class);

    private String host;
    private int port;
    private int timeout;
    private int database;
    private String password;

    private int maxTotal;
    private int maxIdle;
    private int maxWaitMillis;
    private boolean testOnBorrow;
    private boolean testOnReturn;

    private RedisConfig() {}

    private RedisConfig(Builder builder) {}

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getDatabase() {
        return database;
    }

    public String getPassword() {
        return password;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public int getMaxWaitMillis() {
        return maxWaitMillis;
    }

    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }

    public boolean isTestOnReturn() {
        return testOnReturn;
    }

    public static class Builder{

        private Properties properties = new Properties();

        //初始化服务器连接信息
        private String host = properties.getProperty("redis.host");
        private int port = Integer.valueOf(properties.getProperty("redis.port"));
        private int timeout = Integer.valueOf(properties.getProperty("redis.timeout"));
        private int database = Integer.valueOf(properties.getProperty("redis.db"));
        private String password = properties.getProperty("redis.password");

        //初始化连接池配置信息
        private int maxTotal = Integer.valueOf(properties.getProperty("jedis.pool.maxActive"));
        private int maxIdle = Integer.valueOf(properties.getProperty("jedis.pool.maxIdle"));
        private int maxWaitMillis = Integer.valueOf(properties.getProperty("jedis.pool.maxWait"));
        private boolean testOnBorrow = Boolean.valueOf(properties.getProperty("jedis.pool.testOnBorrow"));
        private boolean testOnReturn = Boolean.valueOf(properties.getProperty("jedis.pool.testOnReturn"));

        public Builder() {
            try {
                properties.load(this.getClass().getClassLoader().getResourceAsStream(Constant.REDIS_CONFIG));
                logger.debug("The information of connection is initial completely.");
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setMaxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        public Builder setMaxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        public Builder setMaxWaitMillis(int maxWaitMillis) {
            this.maxWaitMillis = maxWaitMillis;
            return this;
        }

        public Builder setTestOnBorrow(boolean testOnBorrow) {
            this.testOnBorrow = testOnBorrow;
            return this;
        }

        public Builder setTestOnReturn(boolean testOnReturn) {
            this.testOnReturn = testOnReturn;
            return this;
        }

        public RedisConfig build() {
            return new RedisConfig(this);
        }
    }
}
