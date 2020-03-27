import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

public class JedisTest {

    Jedis jedis;

    @Before
    public void setUp(){
        jedis = new Jedis("192.168.50.68",6379);
        System.out.println("连接redis。。。。。。。。");
    }

    @Test
    public void TestJedisList(){
        jedis.flushDB();
        jedis.lpush("vids","01");
        jedis.lpush("vids","03");

    }

    @After
    public void tearDown(){
        jedis.close();
    }
}
