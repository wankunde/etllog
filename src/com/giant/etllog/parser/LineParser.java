package com.giant.etllog.parser;

import java.nio.file.Files;
import java.nio.file.Path;

import com.google.common.base.Preconditions;

public abstract class LineParser {

	public abstract void initParser() throws Exception;

	public abstract String getHdfsPath(Path path) throws Exception;
	
	public abstract String parseLine(Path path, String line) ;
	
	public static ObjscenesParser objscenesParser = new ObjscenesParser();
	public static RegParser regParser = new RegParser();
	public static UseractParser useractParser = new UseractParser();

	public static LineParser getLineParser(Path path) throws Exception {
		Preconditions.checkArgument(Files.exists(path),"文件路径不能为空!");
		String filename = path.getFileName().toString();
		LineParser parser = null;
		if (filename.startsWith("objscenesserver"))
			parser = objscenesParser;
		else if (filename.startsWith("regaccount"))
			parser = regParser;
		else if (filename.startsWith("useract"))
			parser = useractParser;
		else 
			throw new Exception("未找到日志解析器! filename:"+filename);
		parser.initParser();
		return parser;
	}
}