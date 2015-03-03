import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import com.giant.etllog.util.ConfigUtil;
import com.google.common.base.Strings;


public class Test {

	public static void main(String[] args) {
		ConfigUtil.loadConfig("objscenes.xml");
		
		Path path = Paths.get("/opt/testdata/10.29.201.18/00-00-ac-1d-c9-12/tmp/ScenServer04-ZH31/log/statobjscenesserver32.log.150105-20");
		String matches = ConfigUtil.get("monitor.matches");
		System.out.println(matches);
		System.out.println(path.getFileName().toString());
		if (Strings.isNullOrEmpty(matches))
			System.out.println("end");
		else
			System.out.println(path.getFileName().toString().matches(matches));
	}

}
