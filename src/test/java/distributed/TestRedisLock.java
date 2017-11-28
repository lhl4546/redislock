package distributed;

import distributed.lock.RedisLock;
import distributed.lock.RedisOp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

import java.util.UUID;

public class TestRedisLock {
    private RedisLock lock;

    @Before
    public void prepare() {
        JedisPool pool = new JedisPool("192.168.0.104", 6379);
        RedisOp redisOp = new RedisOp(pool);
        lock = new RedisLock(redisOp);
    }

    @Test
    public void test() {
        String key = "org.fire";
        String value = UUID.randomUUID().toString();
        lock.lock(key, value, 10);
        boolean unlock = lock.unlock(key, value);
        Assert.assertTrue(unlock);
        lock.lock(key, value, 10);
        boolean trylock = lock.tryLock(key, value, 10);
        Assert.assertFalse(trylock);
        lock.unlock(key, value);
        trylock = lock.tryLock(key, value, 10);
        Assert.assertTrue(trylock);
        unlock = lock.unlock(key, value);
        Assert.assertTrue(unlock);
    }
}
