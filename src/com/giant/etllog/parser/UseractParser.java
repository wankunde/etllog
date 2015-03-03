package com.giant.etllog.parser;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.giant.etllog.util.CommonUtil;
import com.giant.etllog.util.ConfigUtil;
import com.google.common.base.Joiner;

public class UseractParser extends LineParser {

	private final static Logger logger = LoggerFactory.getLogger(UseractParser.class);
	
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

		String hdfsname = filename.substring(filename.indexOf(".log.") + 5);
		String hdfsdir = hdfsname.substring(0, 6);

		return prefix + "/" + hdfsdir + "/" + hdfsname;
	}

	@Override
	public String parseLine(Path path,String line) {
		String ip = CommonUtil.findIp(path);

		String[] ss = path.getFileName().toString().split("_");
		String game = new String();
		if (ss.length > 2)
			game = ss[1];

		String[] fields;
		if (game.equals("1") || game.equals("20") || game.equals("39"))
			fields = line.split(",");
		else
			fields = line.split("\\t");

		if (fields.length < 25) {
			logger.warn("字段分割没有25个字段 \t" + line);
			return null;
		}
		if (fields[0].length() < 16) {
			logger.warn("数据转换出错:第一个字段长度<15 \t" + line);
			return null;
		}

		// 对int类型字段如果为空,转换为0
		convertInt(fields, 2, 3, 4, 6, 7, 8, 12, 13, 14, 18, 19, 20, 21);

		String logdate = "20" + fields[0].substring(0, 2) + "-" + fields[0].substring(2, 4) + "-"
				+ fields[0].substring(4, 6);
		String logtime = logdate + " " + fields[0].substring(7, 15);
		return Joiner.on("\t").join(new String[] { game, ip, logdate, logtime }) + "\t"
				+ Joiner.on("\t").join(Arrays.copyOfRange(fields, 1, fields.length)) + "\n";
	}



	private String[] convertInt(String[] arr, int... is) {
		for (int i : is) {
			if (arr[i].trim().equals(""))
				arr[i] = "0";
		}
		return arr;
	}

}
