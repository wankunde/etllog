package com.giant.etllog.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class TestParse {

	@Test
	public void parseLine() {
		// r'(\[[0-9-]{10} [0-9:]{7,8}\]) --(.*)$'
		// Pattern pattern =
		// Pattern.compile("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) --(.*+)");
		// Matcher matcher = pattern.matcher("127.400.600.2");
		// while(matcher.find()){
		// System.out.println(matcher.group());
		// }
		// System.out.println(matcher.matches());
		String line = "[2014-09-14 20:02:03] -- jd_site	21	0	2344690	123.151.136.49	6287853	392763677	24		2014-09-14 19:10:21	111.142.196.49	1409100931131603364257		http://reg.ztgame.com/registe/register.jsp?source=jd_site	%C2%BD%D6%AA%D4%B6	18	19930904	0	331081	[Ljavax.servlet.http.Cookie;@2eb6fb28	BC05C099DC5A7D961FEDAF3DD8790195";
		String dt = line.substring(1, 20);
		String regyear = dt.substring(0, 4);
		String regdate = dt.substring(0, 10);
		String regtime = dt;
		System.out.println(regyear);
		System.out.println(regdate);
		System.out.println(regtime);

		line = line.substring(24);
		String[] arrs = line.split("\t");
		if (arrs.length < 4)
			arrs = line.split(",");
		
		System.out.println(arrs[5]);
		System.out.println(arrs[6]);
		System.out.println(arrs[7]);
		System.out.println(line);
		System.out.println(dt);
	}
}
