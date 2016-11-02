package learn.hadoop.hdfs.write;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

/**
 * HDFS Write Demo.
 * @author zhangdong
 * @createtime 2016-11-02 16:33
 * @location peking natural lab
 * */
public class HDFSWriteDemo {

	
	//FSDataOutputStream 
	@Test
	public void writeOne() throws Exception{
		String localSrc = "C:\\Users\\myself\\Desktop\\大数据Spark企业级实战版.pdf";
		String distSrc = "hdfs://master:9000/write2";
		InputStream in  = new BufferedInputStream(new FileInputStream(localSrc));
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(distSrc), conf);
		FSDataOutputStream out = fs.create(new Path(distSrc), 
				new Progressable() {
			        //
					public void progress() {
						System.out.print(">");
					}
				});
		
		IOUtils.copyBytes(in, out, conf);
	}
	
}
