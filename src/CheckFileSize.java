import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * java -classpath ./libs/*:logserver-1.0.0.jar:. CheckFileSize
 * 
 * hadoop dfs -cat /ztgame/zt/a_log/useract_test2/140719-06 | wc -l
 * 
 * find . -name useract*140719-06 |xargs wc -l |grep total
 * 
 * javac -cp ./*:./libs/* CheckFileSize.java
 * 
 * java -cp ./*:./libs/* CheckFileSize
 * 
 * @author wankun
 * @date 2014年7月21日
 * @version 1.0
 */
public class CheckFileSize {

	final static Map<String, Long> filesmap = new HashMap<>();
	final static Map<String, Long> redismap = new HashMap<>();

	public void queryRedis(String pattern) {

		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(10);
		config.setTestOnBorrow(true);
		JedisPool pool = new JedisPool(config, "10.10.102.151", 6379);
		// Jedis jedis = new Jedis("10.10.102.151", 6379);
		Jedis jedis = pool.getResource();
		Set<String> keys = jedis.keys(pattern);
		String[] arrays = keys.toArray(new String[keys.size()]);
		List<String> values = jedis.mget(arrays);

		for (int i = 0; i < arrays.length; i++) {
			redismap.put(arrays[i], Long.parseLong(values.get(i)));
		}
		pool.returnResource(jedis);
		pool.destroy();
	}

	public void walkFileTree(Path rootPath, final String ptn) {
		try {
			Files.walkFileTree(rootPath, new SimpleFileVisitor<Path>() {
				Pattern pattern = Pattern.compile(ptn);

				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					String filename = file.toRealPath().getFileName().toString();
					Matcher matcher = pattern.matcher(filename);
					if (matcher.matches()) {
						filesmap.put(file.toString(), Files.size(file));
					}
					return FileVisitResult.CONTINUE;
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Map<String, String> mapminus(Map<String, Long> map1, Map<String, Long> map2) {
		Map<String, String> res = new HashMap<>();
		Set<Entry<String, Long>> set = map2.entrySet();
		for (Entry<String, Long> en : set) {
			// key redis 有,file 无
			if (!map1.containsKey(en.getKey()))
				res.put(en.getKey(), "r:" + en.getValue() + "  f:0" + "  差额:" + en.getValue());
			// key 都有 值不同
			if (map1.containsKey(en.getKey()) && !map1.get(en.getKey()).equals(en.getValue()))
				res.put(en.getKey(), "r:" + en.getValue() + "  f:" + map1.get(en.getKey()) + " 差额:"
						+ (en.getValue() - map1.get(en.getKey())));
		}
		set = map1.entrySet();
		for (Entry<String, Long> en : set) {
			// key redis 无,file 有
			if (!map1.containsKey(en.getKey()))
				res.put(en.getKey(), "r:0" + "  f:" + en.getValue());
		}
		return res;
	}

	public static void main(String[] args) {
		CheckFileSize check = new CheckFileSize();
		check.queryRedis("*objscenesserver*150106-23");
		check.walkFileTree(Paths.get("/opt/testdata"), "^objscenesserver.*150106-23");
		System.out.println("redis num:" + redismap.size() + "\t  file num:" + filesmap.size());
		System.out.println("------- 数据对比----------");
		Map<String, String> res = check.mapminus(filesmap, redismap);
		for (Entry<String, String> en : res.entrySet()) {
			System.out.println(en.getKey() + "--->" + en.getValue());
		}
	}

	public void testQueryRedis() {
		queryRedis("*useract*140719-06");
	}

	public void testWalkFileTree() {
		walkFileTree(Paths.get("C:\\Users\\wankun\\Desktop\\zmq"), "^mq.*");
		Set<Entry<String, Long>> set = filesmap.entrySet();
		for (Entry<String, Long> en : set) {
			System.out.println(en.getKey() + "--->" + en.getValue());
		}
	}

	public void testMapminus() {
		Map<String, Long> map1 = new HashMap();
		Map<String, Long> map2 = new HashMap();
		map1.put("a", 10l);
		map1.put("b", 20l);
		map2.put("a", 10l);
		map2.put("c", 10l);
		Map<String, String> res = mapminus(map2, map1);
		for (Entry<String, String> en : res.entrySet()) {
			System.out.println(en.getKey() + "--->" + en.getValue());
		}

	}

}
