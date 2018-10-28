import redis.clients.jedis.Jedis;
import utils.RedisUtil;

/**
 * project: testredis
 * package: PACKAGE_NAME
 * Author: yespon
 * Time: 2018/10/26 22:25
 */
public class JavaTest {
//    @Test
//    public void test1(){
//        String []ss = new String[10];
//        System.out.println(ss.length);
//    }

    public static void main(String[] args) {
        String []ss = new String[10];
        System.out.println(ss.length);
        Jedis jedis = RedisUtil.getConnection(RedisUtil.initPool("172.16.21.28",6379,10000,"123456",9));

        for (int i = 0; i < ss.length; i++) {
            ss[i] = "111" + String.valueOf(i);
        }


        RedisUtil.mset(jedis, ss);

        RedisUtil.closeConnection(jedis);
    }
}
