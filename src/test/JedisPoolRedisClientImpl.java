/**
 * Copyright 2007, NetworkBench Systems Corp.
 */

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author BurningIce
 *
 */
public class JedisPoolRedisClientImpl implements RedisClient {
//	private final static Logger logger = LoggerFactory.getLogger(JedisPoolRedisClientImpl.class);
	private final static long STATS_SYNC_INTERVAL = 300000L;
	private final static int MAX_MGET_SIZE = 256;		// mget/mset 单次限制最多key数量
	private final static int MAX_MSET_SIZE = 128 * 2;	// mget/mset 单次限制最多keyvalue数量（mset为键值对，因此，MAX_MSET_SIZE必须为2的整数倍）
	private JedisPool jedisPool;
	private GenericObjectPoolConfig poolConfig;
	private String host;
	private int port = Protocol.DEFAULT_PORT;
	private String password;
	private int timeout = Protocol.DEFAULT_TIMEOUT;
	private int database = Protocol.DEFAULT_DATABASE;
	private boolean statsEnabled = true;
	private ConcurrentHashMap<String, AtomicLong> localStats = new ConcurrentHashMap<String, AtomicLong>();
	private long lastStatSyncTime = System.currentTimeMillis();
	private ReadWriteLock lock = new ReentrantReadWriteLock();
	
	/**
	 * @param poolConfig the poolConfig to set
	 */
	public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
		this.poolConfig = poolConfig;
	}

	/**
	 * @param host the host to set
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		if(password != null && password.trim().length() > 0){
			this.password = password.trim();
		} else {
			this.password = null;
		}
	}

	/**
	 * @param timeout the timeout to set
	 */
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	/**
	 * @param database the database to set
	 */
	public void setDatabase(int database) {
		this.database = database;
	}
	
	/**
	 * @param statsEnabled the statsEnabled to set
	 */
	public void setStatsEnabled(boolean statsEnabled) {
		this.statsEnabled = statsEnabled;
	}

	public void init() {
		this.jedisPool = new JedisPool(poolConfig, host, port, timeout, password, database);
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.backend.service.RedisClient#get(java.lang.String)
	 */
	@Override
	public String get(String key) {
		if(this.jedisPool == null)
			return null;
		
		Jedis jedis = null;
		String value = null;
		try {
			jedis = jedisPool.getResource();
			value = jedis.get(key);
			
			if(this.statsEnabled && key.length() > 0) {
				this.statGet(key.substring(0, 1));
			}
		} catch(Throwable ex) {
//			logger.error("failed to get value from redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return value;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.backend.service.RedisClient#get(byte[])
	 */
	@Override
	public byte[] get(byte[] key) {
		if(this.jedisPool == null)
			return null;
		
		Jedis jedis = null;
		byte[] value = null;
		try {
			jedis = jedisPool.getResource();
			value = jedis.get(key);
			
			if(this.statsEnabled && key.length > 0) {
				this.statGet(String.valueOf((char)key[0]));
			}
		} catch(Throwable ex) {
//			logger.error("failed to get value from redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return value;
	}

	
	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#mget(java.lang.String[])
	 */
	@Override
	public List<String> mget(String... key) {
		if(this.jedisPool == null)
			return null;
		
		if(key == null) {
			return null;
		}
		
		if(key.length == 0) {
			return new ArrayList<String>(1);
		}
		
		if(key.length > MAX_MGET_SIZE) {
			// 单次限制最多mget数量，防止key过多时超时
			List<String> allValues = new ArrayList<String>(key.length);
			int batchedCount = 0;
			String[] batchedKeys = new String[MAX_MGET_SIZE];
			for(String k : key) {
				batchedKeys[batchedCount] = k;
				++batchedCount;
				
				if(batchedCount == MAX_MGET_SIZE) {
					List<String> values = mget(batchedKeys);
					allValues.addAll(values);
					
					batchedCount = 0;
					// 复用batchedKeys数组可适当提升性能，不需要重新初始化，自动覆盖即可
				}
			}
			
			// 剩余的key
			if(batchedCount > 0) {
				String[] remainedKeys;
				if(batchedCount < MAX_MGET_SIZE) {
					remainedKeys = new String[batchedCount];
					System.arraycopy(batchedKeys, 0, remainedKeys, 0, batchedCount);
				} else {
					remainedKeys = batchedKeys;
				}
				
				List<String> values = mget(remainedKeys);
				allValues.addAll(values);
			}
			
			return allValues;
		}
		
		Jedis jedis = null;
		List<String> value = null;
		try {
			jedis = jedisPool.getResource();
			value = jedis.mget(key);
			
			if(this.statsEnabled) {
				if(key.length > 0) {
					for(String k : key) {
						this.statGet(k.substring(0, 1));
					}
				}
			}
		} catch(Throwable ex) {
//			logger.error("failed to mget value from redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return value;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#mget(byte[][])
	 */
	@Override
	public List<byte[]> mget(byte[]... key) {
		if(this.jedisPool == null)
			return null;
		
		if(key == null) {
			return null;
		}
		
		if(key.length == 0) {
			return new ArrayList<byte[]>(1);
		}
		
		if(key.length > MAX_MGET_SIZE) {
			// 单次限制最多mget数量，防止key过多时超时
			List<byte[]> allValues = new ArrayList<byte[]>(key.length);
			int batchedCount = 0;
			byte[][] batchedKeys = new byte[MAX_MGET_SIZE][];
			for(byte[] k : key) {
				batchedKeys[batchedCount] = k;
				++batchedCount;
				
				if(batchedCount == MAX_MGET_SIZE) {
					List<byte[]> values = mget(batchedKeys);
					allValues.addAll(values);
					
					batchedCount = 0;
					// 复用batchedKeys数组可适当提升性能，不需要重新初始化，自动覆盖即可
				}
			}
			
			// 剩余的key
			if(batchedCount > 0) {
				byte[][] remainedKeys;
				if(batchedCount < MAX_MGET_SIZE) {
					remainedKeys = new byte[batchedCount][];
					System.arraycopy(batchedKeys, 0, remainedKeys, 0, batchedCount);
				} else {
					remainedKeys = batchedKeys;
				}
				
				List<byte[]> values = mget(remainedKeys);
				allValues.addAll(values);
			}
			
			return allValues;
		}
		
		Jedis jedis = null;
		List<byte[]> value = null;
		try {
			jedis = jedisPool.getResource();
			value = jedis.mget(key);
			
			if(this.statsEnabled) {
				if(key.length > 0) {
					for(byte[] k : key) {
						this.statGet(String.valueOf((char)k[0]));
					}
				}
			}
		} catch(Throwable ex) {
//			logger.error("failed to mget value from redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return value;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.backend.service.RedisClient#set(java.lang.String, java.lang.String)
	 */
	@Override
	public void set(String key, String value) {
		if(this.jedisPool == null)
			return;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			jedis.set(key, value);
			
			if(this.statsEnabled && key.length() > 0) {
				this.statSet(key.substring(0, 1), value.length());
			}
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.backend.service.RedisClient#set(byte[], byte[])
	 */
	@Override
	public void set(byte[] key, byte[] value) {
		if(this.jedisPool == null)
			return;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			jedis.set(key, value);
			
			if(this.statsEnabled && key.length > 0) {
				this.statSet(String.valueOf((char)key[0]), value.length);
			}
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.backend.service.RedisClient#set(java.lang.String, java.lang.String, int)
	 */
	@Override
	public void set(String key, String value, int expirationInSeconds) {
		if(this.jedisPool == null)
			return;
		
		if(expirationInSeconds <= 0) {
			// 0 for never expiring
			this.set(key, value);
			return;
		}
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();			
			jedis.setex(key, expirationInSeconds, value);
			
			if(this.statsEnabled && key.length() > 0) {
				this.statSet(key.substring(0, 1), value.length());
			}
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#mset(java.lang.String[])
	 */
	@Override
	public void mset(String... keyValues) {
		if(this.jedisPool == null)
			return;
		
		if(keyValues == null || keyValues.length == 0) {
			return;
		}
		
		if(keyValues.length > MAX_MSET_SIZE) {
			// 单次限制最多mset数量，防止key过多时超时
			int batchedCount = 0;
			String[] batchedKeyValues = new String[MAX_MSET_SIZE];
			for(String kv : keyValues) {
				batchedKeyValues[batchedCount] = kv;
				++batchedCount;
				
				if(batchedCount == MAX_MSET_SIZE) {
					mset(batchedKeyValues);
					
					batchedCount = 0;
					// 复用batchedKeys数组可适当提升性能，不需要重新初始化，自动覆盖即可
				}
			}
			
			// 剩余的key
			if(batchedCount > 0) {
				String[] remainedKeyValues;
				if(batchedCount < MAX_MSET_SIZE) {
					remainedKeyValues = new String[batchedCount];
					System.arraycopy(batchedKeyValues, 0, remainedKeyValues, 0, batchedCount);
				} else {
					remainedKeyValues = batchedKeyValues;
				}
				
				mset(remainedKeyValues);
			}
		}
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			jedis.mset(keyValues);
			/*
			if(this.statsEnabled && key.length() > 0) {
				this.statSet(key.substring(0, 1), value.length());
			}
			*/
		} catch(Throwable ex) {
//			logger.error("failed to mset value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#mset(byte[][])
	 */
	@Override
	public void mset(byte[]... keyValues) {
		if(this.jedisPool == null)
			return;
		
		if(keyValues == null || keyValues.length == 0) {
			return;
		}
		
		if(keyValues.length > MAX_MSET_SIZE) {
			// 单次限制最多mset数量，防止key过多时超时
			int batchedCount = 0;
			byte[][] batchedKeyValues = new byte[MAX_MSET_SIZE][];
			for(byte[] kv : keyValues) {
				batchedKeyValues[batchedCount] = kv;
				++batchedCount;
				
				if(batchedCount == MAX_MSET_SIZE) {
					mset(batchedKeyValues);
					
					batchedCount = 0;
					// 复用batchedKeys数组可适当提升性能，不需要重新初始化，自动覆盖即可
				}
			}
			
			// 剩余的key
			if(batchedCount > 0) {
				byte[][] remainedKeyValues;
				if(batchedCount < MAX_MSET_SIZE) {
					remainedKeyValues = new byte[batchedCount][];
					System.arraycopy(batchedKeyValues, 0, remainedKeyValues, 0, batchedCount);
				} else {
					remainedKeyValues = batchedKeyValues;
				}
				
				mset(remainedKeyValues);
			}
		}
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			jedis.mset(keyValues);
			/*
			if(this.statsEnabled && key.length() > 0) {
				this.statSet(key.substring(0, 1), value.length());
			}
			*/
		} catch(Throwable ex) {
//			logger.error("failed to mset value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.backend.service.RedisClient#set(byte[], byte[], int)
	 */
	@Override
	public void set(byte[] key, byte[] value, int expirationInSeconds) {
		if(this.jedisPool == null)
			return;
		
		if(expirationInSeconds <= 0) {
			// 0 for never expiring
			this.set(key, value);
			return;
		}
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();			
			jedis.setex(key, expirationInSeconds, value);
			
			if(this.statsEnabled && key.length > 0) {
				this.statSet(String.valueOf((char)key[0]), value.length);
			}
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
	}

	private void statGet(String key) {
		this.lock.readLock().lock();
		try {
			this.statIncrLocal("getsof#" + key);
			this.statIncrLocal("getsof#");
		} catch(Throwable t) {
			// ignore
		} finally {
			this.lock.readLock().unlock();
		}
		
		long now = System.currentTimeMillis();
		if(this.lastStatSyncTime + STATS_SYNC_INTERVAL < now) {
			// sync every 5 min
			this.lastStatSyncTime = now;
			this.syncStatWithRedis();
		}
	}
	
	private void statSet(String key, int size) {
		this.lock.readLock().lock();
		try {
			this.statIncrLocal("sizeof#" + key, (long)size);
			this.statIncrLocal("sizeof#", (long)size);
			this.statIncrLocal("setsof#" + key);
			this.statIncrLocal("setsof#");
		} catch(Throwable t) {
			// ignore
		} finally {
			this.lock.readLock().unlock();
		}
	}
	
	private void statIncrLocal(String key) {
		AtomicLong v = this.localStats.get(key);
		if(v == null) {
			v = new AtomicLong();
			AtomicLong oldValue = this.localStats.putIfAbsent(key, v);
			if(oldValue != null) {
				v = oldValue;
			}
		}
		
		v.incrementAndGet();
	}
	
	private void statIncrLocal(String key, long increment) {
		AtomicLong v = this.localStats.get(key);
		if(v == null) {
			v = new AtomicLong();
			AtomicLong oldValue = this.localStats.putIfAbsent(key, v);
			if(oldValue != null) {
				v = oldValue;
			}
		}
		
		v.addAndGet(increment);
	}
	
	private void syncStatWithRedis() {
		// sync to redis
		if(this.jedisPool == null)
			return;
		
		Map<String, Long> statsSync = new HashMap<String, Long>();
		this.lock.writeLock().lock();
		try {
			for(Map.Entry<String, AtomicLong> e : this.localStats.entrySet()) {
				statsSync.put(e.getKey(), Long.valueOf(e.getValue().longValue()));
				e.getValue().set(0L);
			}
		} catch(Throwable t) {
			// ignore
		} finally {
			this.lock.writeLock().unlock();
		}
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			for(Map.Entry<String, Long> e : statsSync.entrySet()) {
				long increment = e.getValue().longValue();
				try {
					jedis.incrBy(e.getKey(), increment);
				} catch(Throwable t) {
					// failed, set back to local
					this.statIncrLocal(e.getKey(), increment);
				}
			}
		} catch(Throwable ex) {
			// failed to get resource
//			logger.error("failed to sync stats to redis: " + ex.getMessage(), ex);
			// set all values back to local
			for(Map.Entry<String, Long> e : statsSync.entrySet()) {
				this.statIncrLocal(e.getKey(), e.getValue().longValue());
			}
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#expire(java.lang.String, int)
	 */
	@Override
	public void expire(String key, int expirationInSeconds) {
		if(this.jedisPool == null)
			return;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();			
			jedis.expire(key, expirationInSeconds);
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#expire(byte[], int)
	 */
	@Override
	public void expire(byte[] key, int expirationInSeconds) {
		if(this.jedisPool == null)
			return;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();			
			jedis.expire(key, expirationInSeconds);
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
	}

	@Override
	public long del(String key) {
		long flag = 0;
		if(this.jedisPool == null)
			return flag;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			flag = jedis.del(key);
		} catch(Throwable ex) {
//			logger.error("failed to del value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		return flag;
	}

	@Override
	public long del(byte[] key) {
		long flag = 0;
		if(this.jedisPool == null)
			return flag;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			flag = jedis.del(key);
		} catch(Throwable ex) {
//			logger.error("failed to del value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		return flag;
	}
	
	
	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#incr(java.lang.String)
	 */
	@Override
	public long incr(String key) {
		if(this.jedisPool == null)
			return 0L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Long value = jedis.incr(key);
			return value == null ? 0L : value.longValue();
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return 0L;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#incr(byte[])
	 */
	@Override
	public long incr(byte[] key) {
		if(this.jedisPool == null)
			return 0L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Long value = jedis.incr(key);
			return value == null ? 0L : value.longValue();
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return 0L;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#incrBy(java.lang.String, long)
	 */
	@Override
	public long incrBy(String key, long increment) {
		if(this.jedisPool == null)
			return 0L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Long value = jedis.incrBy(key, increment);
			return value == null ? 0L : value.longValue();
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return 0L;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#incrBy(byte[], long)
	 */
	@Override
	public long incrBy(byte[] key, long increment) {
		if(this.jedisPool == null)
			return 0L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Long value = jedis.incrBy(key, increment);
			return value == null ? 0L : value.longValue();
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return 0L;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#decr(java.lang.String)
	 */
	@Override
	public long decr(String key) {
		if(this.jedisPool == null)
			return 0L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Long value = jedis.decr(key);
			return value == null ? 0L : value.longValue();
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return 0L;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#decr(byte[])
	 */
	@Override
	public long decr(byte[] key) {
		if(this.jedisPool == null)
			return 0L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Long value = jedis.decr(key);
			return value == null ? 0L : value.longValue();
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return 0L;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#decrBy(java.lang.String, long)
	 */
	@Override
	public long decrBy(String key, long decrement) {
		if(this.jedisPool == null)
			return 0L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Long value = jedis.decrBy(key, decrement);
			return value == null ? 0L : value.longValue();
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return 0L;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#decrBy(byte[], long)
	 */
	@Override
	public long decrBy(byte[] key, long decrement) {
		if(this.jedisPool == null)
			return 0L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Long value = jedis.decrBy(key, decrement);
			return value == null ? 0L : value.longValue();
		} catch(Throwable ex) {
//			logger.error("failed to set value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return 0L;
	}
	
	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.backend.service.RedisClient#get(java.lang.String)
	 */
	@Override
	public String hget(String key, String field) {
		if(this.jedisPool == null)
			return null;

		Jedis jedis = null;
		String value = null;
		try {
			jedis = jedisPool.getResource();
			value = jedis.hget(key, field);

			if(this.statsEnabled && key.length() > 0) {
				this.statGet(key.substring(0, 1));
			}
		} catch(Throwable ex) {
//			logger.error("failed to hget value from redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}

		return value;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.backend.service.RedisClient#get(byte[])
	 */
	@Override
	public byte[] hget(byte[] key, byte[] field) {
		if(this.jedisPool == null)
			return null;
		
		Jedis jedis = null;
		byte[] value = null;
		try {
			jedis = jedisPool.getResource();
			value = jedis.hget(key, field);
			
			if(this.statsEnabled && key.length > 0) {
				this.statGet(String.valueOf((char)key[0]));
			}
		} catch(Throwable ex) {
//			logger.error("failed to hget value from redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return value;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#hmget(java.lang.String, java.lang.String[])
	 */
	@Override
	public List<String> hmget(String key, String... field) {
		if(this.jedisPool == null)
			return null;
		
		Jedis jedis = null;
		List<String> value = null;
		try {
			jedis = jedisPool.getResource();
			value = jedis.hmget(key, field);
			
			if(this.statsEnabled && key.length() > 0) {
				this.statGet(key.substring(0, 1));
			}
		} catch(Throwable ex) {
//			logger.error("failed to hget value from redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return value;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#hmget(byte[], byte[][])
	 */
	@Override
	public List<byte[]> hmget(byte[] key, byte[]... field) {
		if(this.jedisPool == null)
			return null;
		
		Jedis jedis = null;
		List<byte[]> value = null;
		try {
			jedis = jedisPool.getResource();
			value = jedis.hmget(key, field);
			
			if(this.statsEnabled && key.length > 0) {
				this.statGet(String.valueOf((char)key[0]));
			}
		} catch(Throwable ex) {
//			logger.error("failed to hget value from redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return value;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.backend.service.RedisClient#set(java.lang.String, java.lang.String)
	 */
	@Override
	public void hset(String key, String field, String value) {
		if(this.jedisPool == null)
			return;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			jedis.hset(key, field, value);
			
			if(this.statsEnabled && key.length() > 0) {
				this.statSet(key.substring(0, 1), value.length());
			}
		} catch(Throwable ex) {
//			logger.error("failed to hset value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.backend.service.RedisClient#set(byte[], byte[])
	 */
	@Override
	public void hset(byte[] key, byte[] field, byte[] value) {
		if(this.jedisPool == null)
			return;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			jedis.hset(key, field, value);
			
			if(this.statsEnabled && key.length > 0) {
				this.statSet(String.valueOf((char)key[0]), value.length);
			}
		} catch(Throwable ex) {
//			logger.error("failed to hset value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#hmset(java.lang.String, java.util.Map)
	 */
	@Override
	public void hmset(String key, Map<String, String> fieldAndValues) {
		if(this.jedisPool == null || fieldAndValues == null)
			return;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			jedis.hmset(key, fieldAndValues);
			
			if(this.statsEnabled && key.length() > 0) {
				int valuesLength = 0;
				for(String value : fieldAndValues.values()) {
					valuesLength += value.length();
				}
				
				this.statSet(key.substring(0, 1), valuesLength);
			}
		} catch(Throwable ex) {
//			logger.error("failed to hset value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#hmset(byte[], java.util.Map)
	 */
	@Override
	public void hmset(byte[] key, Map<byte[], byte[]> fieldAndValues) {
		if(this.jedisPool == null)
			return;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			jedis.hmset(key, fieldAndValues);
			
			if(this.statsEnabled && key.length > 0) {
				int valuesLength = 0;
				for(byte[] value : fieldAndValues.values()) {
					valuesLength += value.length;
				}
				
				this.statSet(String.valueOf((char)key[0]), valuesLength);
			}
		} catch(Throwable ex) {
//			logger.error("failed to hset value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
	}

	@Override
	public Map<String, String> hgetAll(String key) {
		if(this.jedisPool == null)
			return null;

		Jedis jedis = null;
		Map<String, String> value = null;
		try {
			jedis = jedisPool.getResource();
			value = jedis.hgetAll(key);

			if(this.statsEnabled && key.length() > 0) {
				this.statGet(key.substring(0, 1));
			}
		} catch(Throwable ex) {
//			logger.error("failed to hget value from redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}

		return value;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#hincrBy(java.lang.String, java.lang.String, long)
	 */
	@Override
	public long hincrBy(String key, String field, long increment) {
		if(this.jedisPool == null)
			return 0L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Long value = jedis.hincrBy(key, field, increment);
			return value == null ? 0L : value.longValue();
		} catch(Throwable ex) {
//			logger.error("failed to hincrBy value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return 0L;
	}

	/* (non-Javadoc)
	 * @see com.networkbench.newlens.datacollector.service.RedisClient#hincrBy(byte[], byte[], long)
	 */
	@Override
	public long hincrBy(byte[] key, byte[] field, long increment) {
		if(this.jedisPool == null)
			return 0L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Long value = jedis.hincrBy(key, field, increment);
			return value == null ? 0L : value.longValue();
		} catch(Throwable ex) {
//			logger.error("failed to hincrBy value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return 0L;
	}

	@Override
	public Long pfAdd(String key, String... elements) {
		if(this.jedisPool == null)
			return 0L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Long value = jedis.pfadd(key, elements);
			return value == null ? 0L : value.longValue();
		} catch(Throwable ex) {
//			logger.error("failed to pfAdd value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return 0L;
	}

	@Override
	public Long pfAdd(String key, int expirationInSeconds, String... elements) {
		if(this.jedisPool == null)
			return 0L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Long value = jedis.pfadd(key, elements);
			jedis.expire(key, expirationInSeconds);
			return value == null ? 0L : value.longValue();
		} catch(Throwable ex) {
//			logger.error("failed to pfAdd value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return 0L;
	}

	@Override
	public long pfCount(String key) {
		if(this.jedisPool == null)
			return 0L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			Long value = jedis.pfcount(key);
			return value == null ? 0L : value.longValue();
		} catch(Throwable ex) {
//			logger.error("failed to pfCount value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return 0L;
	}
	
	@Override
	public String pfmerge(String destkey, String... sourcekeys) {
		if(this.jedisPool == null)
			return null;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			return jedis.pfmerge(destkey, sourcekeys);
		} catch(Throwable ex) {
//			logger.error("failed to pfmerge value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return null;
	}
	
	@Override
	public Long sadd(String key, String... members) {
		if(this.jedisPool == null)
			return -1L;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			return jedis.sadd(key, members);
		} catch(Throwable ex) {
//			logger.error("failed to sadd value to redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return -1L;
	}
	
	@Override
	public Set<String> keys(String pattern) {
		if(this.jedisPool == null)
			return null;
		
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			return jedis.keys(pattern);
		} catch(Throwable ex) {
//			logger.error("failed to keys for redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}
		
		return null;
	}

	@Override
	public Set<String> smembers(String key) {
		if(this.jedisPool == null)
			return null;

		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			return jedis.smembers(key);
		} catch(Throwable ex) {
//			logger.error("failed to keys for redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}

		return null;
	}

	@Override
	public Pipeline pipelined() {
		if(this.jedisPool == null)
			return null;

		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			return jedis.pipelined();
		} catch(Throwable ex) {
//			logger.error("failed to keys for redis: " + ex.getMessage(), ex);
		} finally {
			if(jedis != null) {
				try {
					jedis.close();
				} catch(Throwable t) {
//					logger.error("error to return redis resource to pool: " + t.getMessage(), t);
				}
			}
		}

		return null;
	}

	public void destroy() {
//		logger.info("destroying redis client ...");
		if(this.jedisPool != null) {
			this.jedisPool.destroy();
		}
	}

}
