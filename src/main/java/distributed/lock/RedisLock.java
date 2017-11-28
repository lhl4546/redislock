package distributed.lock;

import java.util.concurrent.TimeUnit;

public class RedisLock {
    private RedisOp redisOp;

    public RedisLock(RedisOp redisOp) {
        this.redisOp = redisOp;
    }

    /**
     * get a lock for the key, wait until the lock is available
     *
     * @param key           lock key
     * @param value         lock value, a unique value will be better
     * @param timeoutSecond timeout in second
     */
    public void lock(String key, String value, int timeoutSecond) {
        String retVal = redisOp.set(key, value, RedisOp.NX, RedisOp.EX, timeoutSecond);
        if ("OK".equals(retVal)) {
            return;
        }

        while (true) {
            retVal = redisOp.set(key, value, RedisOp.NX, RedisOp.EX, timeoutSecond);
            if ("OK".equals(retVal)) {
                return;
            }

            try {
                TimeUnit.MILLISECONDS.sleep(1);
            } catch (InterruptedException ie) {
                // ignore
            }
        }
    }

    /**
     * get a lock for the key, wait until the lock is available or an InterruptedException
     * throws
     *
     * @param key           lock key
     * @param value         lock value
     * @param timeoutSecond timeout in second
     * @throws InterruptedException
     */
    public void lockInterruptibility(String key, String value, int timeoutSecond) throws InterruptedException {
        String retVal = redisOp.set(key, value, RedisOp.NX, RedisOp.EX, timeoutSecond);
        if ("OK".equals(retVal)) {
            return;
        }

        while (true) {
            retVal = redisOp.set(key, value, RedisOp.NX, RedisOp.EX, timeoutSecond);
            if ("OK".equals(retVal)) {
                return;
            }

            TimeUnit.MILLISECONDS.sleep(1);
        }
    }

    /**
     * try to get a lock for the key, return true is success
     *
     * @param key           lock key
     * @param value         lock value
     * @param timeoutSecond timeout in second
     * @return true if lock is available now
     */
    public boolean tryLock(String key, String value, int timeoutSecond) {
        String retVal = redisOp.set(key, value, RedisOp.NX, RedisOp.EX, timeoutSecond);
        return "OK".equals(retVal);
    }

    /**
     * unlock for the key
     *
     * @param key   lock key
     * @param value lock value
     * @return true if lock is successfully free
     */
    public boolean unlock(String key, String value) {
        String script = "if redis.call(\"get\",KEYS[1]) == ARGV[1] then "
                + "return redis.call(\"del\",KEYS[1]) "
                + "else "
                + "return 0 "
                + "end";
        Object retVal = redisOp.eval(script, key, value);
        return retVal != null ? ((Number) retVal).longValue() > 0 : false;
    }
}
