package distributed.lock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisOp {
    /**
     * Only set the key if it does not already exist
     */
    public static final String NX = "nx";
    /**
     * Only set the key if it already exist.
     */
    public static final String XX = "xx";
    /**
     * Set the specified expire time, in seconds.
     */
    public static final String EX = "ex";
    /**
     * Set the specified expire time, in milliseconds.
     */
    public static final String PX = "px";
    private JedisPool jedisPool;

    public RedisOp(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public interface Executor<T> {
        T execute(Jedis jedis);
    }

    protected <T> T execute(Executor<T> executor) {
        try (Jedis jedis = jedisPool.getResource()) {
            return executor.execute(jedis);
        }
    }

    /**
     * Set key to hold the string value. If key already holds a value, it is overwritten,
     * regardless of its type. Any previous time to live associated with the key is
     * discarded on successful SET operation.
     *
     * @param key
     * @param value
     * @param nxxx    see {@link #NX}  {@link #PX}
     * @param expx    see {@link #EX} {@link #PX}
     * @param timeout if ex is set, it is intepreted as second, else if px is set, it is intepreted as millisecond
     * @return
     */
    public String set(String key, String value, String nxxx, String expx, int timeout) {
        return execute(jedis -> jedis.set(key, value, nxxx, expx, timeout));
    }

    /**
     *
     * @param script
     * @param key
     * @param value
     * @return
     */
    public Object eval(String script, String key, String value) {
        return execute(jedis -> jedis.eval(script, 1, key, value));
    }
}
