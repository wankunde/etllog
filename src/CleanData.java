import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import redis.clients.jedis.Jedis;

import com.giant.etllog.util.ConfigUtil;

public class CleanData {
	public static void main(String[] args) {
		try {
			Configuration conf = ConfigUtil.getConf();
			FileSystem hdfs = FileSystem.get(conf);
			hdfs.delete(new Path("/tmp/wankun/reg"), true);

			java.nio.file.Path p = Paths.get("/usr/local/etllog/logs/gc.log");
			if (Files.exists(p))
				Files.delete(Paths.get("/usr/local/etllog/logs/gc.log"));
			p = Paths.get("/usr/local/etllog/logs/logserver.log");
			if (Files.exists(p))
				Files.delete(Paths.get("/usr/local/etllog/logs/logserver.log"));

			Jedis jedis = new Jedis("10.10.102.182", 6379);
			jedis.flushDB();
			jedis.close();

			System.out.println("clean data successful");
		} catch (IOException e) {
			System.out.println("clean data fail");
			e.printStackTrace();
		}
	}
}
