import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map.Entry;

import com.giant.etllog.util.FileReaderUtil;

public class TestReader {
	public static String[] myargs={"F:\\wankun\\workspace\\etllog\\test\\useract_20_c_21.log.141012-23"};
	public static void main(String[] args) {
		try {
			new TestReader().doRead(myargs[0]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void doRead(String filename) throws IOException {
		Path srcPath = Paths.get(filename);

		FileReaderUtil reader = new FileReaderUtil(srcPath);
		reader.reset(0);
		try {
			while (reader.hasNext()) {
				Entry<Long, String> en = reader.next();
				System.out.println(en.getKey() + "--->" + en.getValue());
			}
			Thread.currentThread().sleep(1000);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		Entry<Long, String> en = reader.lastLine();
		if (en != null) {
			System.out.println(en.getKey() + "--->" + en.getValue());
		}
	}

}
