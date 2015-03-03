package com.giant.etllog.parser;

import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.giant.etllog.util.ConfigUtil;
import com.google.common.base.Preconditions;

public class RegParser extends LineParser {
	
	private final static Logger logger = LoggerFactory.getLogger(RegParser.class);

	protected final String HDFS_FILE_PREFIX = ConfigUtil.get("hdfs.file.prefix");

	@Override
	public void initParser() throws Exception {
	}

	@Override
	public String getHdfsPath(Path path) throws Exception {
		String filename = path.getFileName().toString();
		String prefix = HDFS_FILE_PREFIX;
		if (prefix.endsWith("/"))
			prefix = prefix.substring(0, prefix.length() - 1);
		if (filename.indexOf(".log.") < 0) {
			throw new Exception("分割文件名失败,file name:" + filename);
		}

		return prefix + "/" + filename.substring(filename.indexOf(".log.") + 5);
	}

	@Override
	public String parseLine(Path path, String line) {
		try {
			Preconditions.checkNotNull(line, "parse line 不能为空");

			// r'(\[[0-9-]{10} [0-9:]{7,8}\]) --(.*)$'

			Pattern pattern = Pattern
					.compile("\\b((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\.((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\.((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\.((?!\\d\\d\\d)\\d+|1\\d\\d|2[0-4]\\d|25[0-5])\\b");
			Matcher matcher = pattern.matcher("127.400.600.2");
			System.out.println(matcher.matches());

		} catch (Exception e) {
			logger.error("行解析失败  line:" + line, e);
		}
		return null;
	}

}
