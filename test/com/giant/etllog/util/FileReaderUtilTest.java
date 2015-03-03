package com.giant.etllog.util;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map.Entry;

import org.junit.Test;

import com.giant.etllog.parser.LineParser;
import com.giant.etllog.parser.UseractParser;

public class FileReaderUtilTest {
	public static String dir = "test/data/10.10.102.110";
	public static String filename = "useract_20_c_21.log.141012-23";

	@Test
	public void testReadFile() throws IOException {
		FileReaderUtil reader = new FileReaderUtil(Paths.get(dir,filename));

		reader.reset(0);
		int num=0;
		while (reader.hasNext() && (num++)<10 ) {
			Entry<Long, String> en = reader.next();
			System.out.println(en.getValue());
		}

	}
	
	LineParser parser = new UseractParser();
	@Test
	public void testReadLine() throws IOException {
		FileReaderUtil reader = new FileReaderUtil(Paths.get(dir,filename));

		reader.reset(0);
		int num=0;
		while (reader.hasNext() && (num++)<10 ) {
			Entry<Long, String> en = reader.next();
			String conv = parser.parseLine(Paths.get(dir, filename), (String) en.getValue());
			if (conv != null) {
				System.out.println(conv);
			}
		}

		Entry<Long, String> en = reader.lastLine();
		if (en != null) {
			String conv = parser.parseLine(Paths.get(dir, filename), (String) en.getValue());
			if (conv != null) {
				System.out.println(conv);
			}
		}
	}
}
