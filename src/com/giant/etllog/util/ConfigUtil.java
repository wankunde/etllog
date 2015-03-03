package com.giant.etllog.util;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

public class ConfigUtil {
	private static Configuration conf = null;
	static {
		conf = new Configuration();
		conf.addResource("conf/hdfs-site.xml");
		conf.addResource("conf/core-site.xml");
	}

	public static void loadConfig(String filename) {
		conf.addResource(filename);
	}

	public static Configuration getConf() {
		return conf;
	}

	public static String get(String name) {
		return conf.get(name);
	}

	public static String get(String name, String defaultValue) {
		return conf.get(name, defaultValue);
	}

	public static int getInt(String name, int defaultValue) {
		return conf.getInt(name, defaultValue);
	}

	public static long getLong(String name, long defaultValue) {
		return conf.getLong(name, defaultValue);
	}

	public static boolean getBoolean(String name, boolean defaultValue) {
		return conf.getBoolean(name, defaultValue);
	}
}
