import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * @ClassName TestRedis
 * <p>
 * </p>
 * @Author Yespon Liu(yespon@qq.com)
 * @Date 2018/10/23 12:06
 */
public class TestRedis {
    public static void main(String[] args) {

        Jedis jedis = new Jedis("r-2ze5b49d36bd30a4.redis.rds.aliyuncs.com", 6379, 100000);
        jedis.auth("xg28KbjA");
        jedis.select(1);
        double start = System.currentTimeMillis();
        Set<byte[]> stringSet = jedis.keys("*".getBytes());
        byte[][] keys = stringSet.toArray(new byte[stringSet.size()][]);

        byte[][] values = jedis.mget(keys).toArray(new byte[stringSet.size()][]);

        for (int i = 0; i < stringSet.size(); ++i) {
//            if (keys[i] != null && values[i] != null) {
//                System.out.println(new String(keys[i]) + " --- " + new String(values[i]));
//            } else if (keys[i] != null) {
//                System.out.println(new String(keys[i]) + " --- " + " ");
//            }
            if (keys[i] != null && values[i] != null) {
                System.out.println(new String(keys[i]) + " --- " + values[i]);
            } else if (keys[i] != null) {
                System.out.println(new String(keys[i]) + " --- " + " ");
            }
        }

//        int count = 0;
//        Iterator iterator = stringSet.iterator();
//        ArrayList<byte[]> keys = new ArrayList<>(stringSet);
//        while(iterator.hasNext()) {
//            String value = jedis.get(String.valueOf(iterator.next()));
//
//            System.out.println(value);
//
//            System.out.println(jedis.mget(keys));
//            count++;
//        }
//        System.out.println("Total values:" + count);
        double end = System.currentTimeMillis();
        System.out.println("Total time:" + (end - start));
        jedis.close();
    }


}
