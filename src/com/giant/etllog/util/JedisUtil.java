package com.giant.etllog.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.google.common.base.Strings;

/**
 * hadoop jar logserver-1.0.0.jar com.giant.logserver.util.JedisUtil
 * 
 * @author wankun
 * @date 2014年6月29日
 * @version 1.0
 */
public class JedisUtil {
	
	private final static Logger logger = LoggerFactory.getLogger(JedisUtil.class);

	private static final String REDIS_IP = ConfigUtil.get("redis.ip");
	private static final int REDIS_PORT = ConfigUtil.getInt("redis.port", 6379);

	private static JedisPool pool = null;

	private synchronized static void checkOpen() {
		if (pool == null) {
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxIdle(10);
			config.setTestOnBorrow(true);
			pool = new JedisPool(config, REDIS_IP, REDIS_PORT);
		}
	}

	public synchronized static <T> boolean put(String key, T value) throws Exception {
		checkOpen();
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			if (value instanceof String)
				jedis.set(key, (String) value);
			else if (value instanceof Long)
				jedis.set(key, "" + (Long) value);
			else if (value instanceof Integer)
				jedis.set(key, "" + (Integer) value);
			else {
				logger.error("类型转换失败  value:" + value);
				return false;
			}
			return true;
		} catch (Exception e) {
			logger.error("redis数据保存失败", e);
			throw e;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public synchronized static Long get(String key) throws Exception {
		checkOpen();
		Jedis jedis = null;
		String value = null;
		try {
			jedis = pool.getResource();
			value = jedis.get(key);
			if (!Strings.isNullOrEmpty(value))
				return Long.parseLong(value);
			else
				return 0l;
		} catch (Exception e) {
			logger.error("redis数据查询失败 key:" + key + " \t value:" + value, e);
			throw e;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public synchronized static Map<String,Long> mget(String[] keys) throws Exception {
		checkOpen();
		Jedis jedis = null;
		Map<String,Long> values = null;
		try {
			jedis = pool.getResource();
			List<String> res = jedis.mget(keys);
			// 如果查询结果数量不够,返回null
			if (res == null || res.size() != keys.length) 
				throw new Exception("redis 查询结果条数错误");
				
			values = new HashMap<>();
			for(int i=0;i<keys.length;i++)
			{
				String s=res.get(i);
				if (!Strings.isNullOrEmpty(s))
					values.put(keys[i],Long.parseLong(s));
				else
					values.put(keys[i],0l);
			}
			return values;
		} catch (Exception e) {
			logger.error("redis数据查询失败 ", e);
			throw e;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public static void main(String[] args) throws Exception {
		// put
		JedisUtil.put("a", 100l);
		System.out.println(JedisUtil.get("a"));
	}

}
