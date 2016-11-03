package learn.hadoop.hdfs.consistentmodel;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.common.IOUtils;
import org.junit.Test;

/**
 * 读写一致性.
 * @author zhangdong
 * @createtime 2016-11-03 09:28
 * @location peking national library
 * 
 * HDFS的一致模型可以这样理解: 
 *  一个数据库是一个事务范围， 当写入数据的大小在一个数据块范围内时，写入数据对外界不可见。
 *  可以调用FSDataOutputStream 的 sync方法或close反复来强制将数据刷新到磁盘。
 * 
 * */
public class ConsistentModel {
     
	@Test
	public void hdfsConsistentModelTest() throws Exception{
		
		//将本地文件写入hffs
		String localSrc = "E:\\New_WorkSpace\\eclipse\\bigdata_framework_hadoop\\resources\\log4j.properties";
		String hdfsSrc = "hdfs://master:9000/consistentmodel1/1/a.txt";
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));		
	    
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(hdfsSrc), conf);
		Path path = new Path(hdfsSrc);
	    FSDataOutputStream out = fs.create(path);
	
	    //IOUtils.copy(in, out, 1024);
	    IOUtils.copyBytes(in, out, 1024);
	    System.out.println(fs.exists(path) +  "已存在");
	    System.out.println((fs.getFileStatus(path).getLen() == 0) +  "长度为0");
        out.sync();
        System.out.println((fs.getFileStatus(path).getLen() != 0) + "sync-> 不为0");
        IOUtils.closeStream(out);
	    
	}
}
