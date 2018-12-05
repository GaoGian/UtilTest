
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisPoolConfig;


public class RedisTest {

	public static void main(String[] args) {
		JedisPoolRedisClientImpl redisClient = new JedisPoolRedisClientImpl();
		GenericObjectPoolConfig poolConfig = new JedisPoolConfig();
		redisClient.setHost("dev-redis.tingyun.com");
		redisClient.setPassword("nbs!@#123");
		redisClient.setPoolConfig(poolConfig);
		redisClient.init();
		
//		String redisVal = redisClient.get("SERVER_LIMIT_AGREEMENT#1760");
		redisClient.set("expire_test", "expire_test", 600);
		System.out.println("------------------------");

	}
}
