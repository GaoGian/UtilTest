/**
 * Copyright 2007, NetworkBench Systems Corp.
 */

import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Redis客户端封装
 * 
 * @author BurningIce
 *
 */
public interface RedisClient {
	/**
	 * get
	 * @param key
	 * @return
	 */
	public String get(String key);
	
	/**
	 * get
	 * @param key
	 * @return
	 */
	public byte[] get(byte[] key);
	
	/**
	 * get
	 * @param key
	 * @return
	 */
	public List<String> mget(String... key);
	
	/**
	 * get
	 * @param key
	 * @return
	 */
	public List<byte[]> mget(byte[]... key);
	
	/**
	 * del
	 * @param key
	 * @return
	 */
	public long del(String key);
	
	/**
	 * del
	 * @param key
	 * @return
	 */
	public long del(byte[] key);
	
	/**
	 * set
	 * @param key
	 * @param value
	 */
	public void set(String key, String value);
	
	/**
	 * set
	 * @param key
	 * @param value
	 */
	public void set(byte[] key, byte[] value);
	
	/**
	 * set and expire (setex)
	 * @param key
	 * @param value
	 * @param expirationInSeconds expiration in seconds
	 */
	public void set(String key, String value, int expirationInSeconds);
	
	/**
	 * set and expire (setex)
	 * @param key
	 * @param value
	 * @param expirationInSeconds expiration in seconds
	 */
	public void set(byte[] key, byte[] value, int expirationInSeconds);

	public void mset(String... keyValues);

	public void mset(byte[]... keyValues);
	
	/**
	 * 设置过期时间
	 * @param key
	 * @param expirationInSeconds 过期时间（单位：秒）
	 */
	public void expire(String key, int expirationInSeconds);
	
	/**
	 * 设置过期时间
	 * @param key
	 * @param expirationInSeconds 过期时间（单位：秒）
	 */
	public void expire(byte[] key, int expirationInSeconds);	
	
	/**
	 * 指定Key自增
	 * @param key
	 * @return 自增后的值
	 */
	public long incr(String key);
	/**
	 * 指定Key自增
	 * @param key
	 * @return
	 */
	public long incr(byte[] key);
	/**
	 * 指定Key自增
	 * @param key
	 * @return
	 */
	public long incrBy(String key, long increment);
	/**
	 * 指定Key自增
	 * @param key
	 * @return
	 */
	public long incrBy(byte[] key, long increment);
	
	/**
	 * 指定Key自减
	 * @param key
	 * @return 自减后的值
	 */
	public long decr(String key);
	/**
	 * 指定Key自减
	 * @param key
	 * @return
	 */
	public long decr(byte[] key);
	/**
	 * 指定Key自减
	 * @param key
	 * @return
	 */
	public long decrBy(String key, long decrement);
	/**
	 * 指定Key自减
	 * @param key
	 * @return
	 */
	public long decrBy(byte[] key, long decrement);
	
	/**
	 * hget
	 * @param key
	 * @return
	 */
	public String hget(String key, String field);
	
	/**
	 * hget
	 * @param key
	 * @return
	 */
	public byte[] hget(byte[] key, byte[] field);
	
	/**
	 * hget
	 * @param key
	 * @return
	 */
	public List<String> hmget(String key, String... field);
	
	/**
	 * hget
	 * @param key
	 * @return
	 */
	public List<byte[]> hmget(byte[] key, byte[]... field);
	
	/**
	 * hset
	 * @param key
	 * @param value
	 */
	public void hset(String key, String field, String value);
	
	/**
	 * hset
	 * @param key
	 * @param value
	 */
	public void hset(byte[] key, byte[] field, byte[] value);


	public void hmset(String key, Map<String, String> fieldAndValues);
	

	public void hmset(byte[] key, Map<byte[], byte[]> fieldAndValues);

	public Map<String, String> hgetAll(String key);

	/**
	 * 指定Key自增
	 * @param key
	 * @return
	 */
	public long hincrBy(String key, String field, long increment);
	/**
	 * 指定Key自增
	 * @param key
	 * @return
	 */
	public long hincrBy(byte[] key, byte[] field, long increment);
	

	public Long pfAdd(String key, String... elements);
	
	/**
	 * 
	 * @param key
	 * @param elements
	 * @param expirationInSeconds 过期时间（单位：秒）
	 * @return
	 */
	public Long pfAdd(String key, int expirationInSeconds, String... elements);
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public long pfCount(String key);
	
	/**
	 * 
	 * @param destkey 将其他key的值meger到此key
	 * @param sourcekeys
	 * @return
	 */
	public String pfmerge(String destkey, String... sourcekeys);
	
	/**
	 * redis set集合
	 * @param key
	 * @param members
	 * @return
	 */
	public Long sadd(final String key, final String... members);
	
	/**
	 * key查询
	 * @param pattern
	 * @return
	 */
	public Set<String> keys(final String pattern);

	/**
	 * 返回key集合所有的元素.
	 * @param key
	 * @return
     */
	public Set<String> smembers(String key);

	public Pipeline pipelined();
}
