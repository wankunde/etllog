package com.giant.etllog.parser;

import org.junit.Test;

public class GameTypeServiceTest {
	
	@Test
	public void test1(){
		String hostname = "ALLServer-ZH61";
		String idcname=null;
		String[] sp = hostname.split("-");
		if (sp.length == 2)
			idcname = sp[1];
		if (sp.length == 3)
			idcname = hostname.substring(0, hostname.lastIndexOf("-"));
		System.out.println(idcname);
	}
	
}
