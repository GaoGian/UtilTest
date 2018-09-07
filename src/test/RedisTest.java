
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisPoolConfig;


public class RedisTest {

	public static void main(String[] args) {
		JedisPoolRedisClientImpl redisClient = new JedisPoolRedisClientImpl();
		GenericObjectPoolConfig poolConfig = new JedisPoolConfig();
		redisClient.setHost("192.168.2.23");
		redisClient.setPassword("");
		redisClient.setPoolConfig(poolConfig);
		redisClient.init();
		
		String redisVal = redisClient.get("SERVER_LIMIT_AGREEMENT#1760");
		System.out.println("------------------------");

	}
}
