import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.giant.etllog.util.ConfigUtil;

public class CleanLease {
	private final static Logger logger = LoggerFactory.getLogger(CleanLease.class);
	private static DistributedFileSystem hdfs = null;

	public static void main(String[] args) {
		if (args == null || args.length < 1 || args[0] == null || args[0].length() == 0)
			logger.info(" Usage: hadoop jar logserver-1.0.0.jar CleanLease [hdfspath]");
		try {
			hdfs = (DistributedFileSystem) FileSystem.get(ConfigUtil.getConf());
			String dstpath = args[0];
			logger.info("try to recover file Lease : " + dstpath);
			hdfs.recoverLease(new Path(dstpath));
			boolean isclosed = hdfs.isFileClosed(new Path(dstpath));
			while (!isclosed) {
				try {
					Thread.currentThread().sleep(1000);
				} catch (InterruptedException e1) {
				}
				isclosed = hdfs.isFileClosed(new Path(dstpath));
			}
			logger.info("file is closed" + dstpath);
		} catch (IOException e) {
			logger.error("HDFS FileSystem error", e);
		}

	}

}
