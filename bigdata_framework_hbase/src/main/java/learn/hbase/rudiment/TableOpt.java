package learn.hbase.rudiment;

import java.io.IOException; 
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * HBase Table rudiment operation.
 * 1.create table
 * 2.put/delete/scan/update
 * 
 * @author zhangdong
 * @createtime 2016-10-30 15:57
 * @location peking
 * */
public class TableOpt {
	
	private static final Log logger = LogFactory.getLog(TableOpt.class);

	private Map<String, String> optException = new HashMap<String, String>();
	private Configuration configuration;
	private Connection conn;
	
	/**
	 * Init HBase configuration and connection.
	 * <p>
	 *     Parameter hbase.zookeeper.property.clientPort mapping zookeeper service port.<br>
	 *     Parameter hbase.zookeeper.quorum mapping znode of zookeeper cluster ips.<br>
	 *     Parameter hbase.master mapping HBase Master service on HBase cluster.</br>
	 * </p>
	 * */
	@Before
	public void init(){
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum","hbase");
		configuration.set("hbase.master", "hbase:60000");
		try {
			conn = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			optException.put("init()", e.getMessage());
		}
	}
	
	/**
	 * clean up operations
	 * */
	@After
	public void optfinish(){
		//clean up resources
		cleanup();
	}
	
	
	
	/**
	 * Create table on HBase.
	 * <p>
	 *  Table name and column family must be given when create table on HBase.
	 * </p>
	 * */
	@Test
	public void createTable() throws Exception{
		
		String tableName = "ln_test";
		String columnFamily = "colfamily_one";
	
		HBaseAdmin admin = (HBaseAdmin)conn.getAdmin();
		//create reference of hbase table
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
		//add column family to table
		table.addFamily(new HColumnDescriptor(columnFamily));
		
		//create table 
		if(admin.tableExists(tableName)){
			System.out.println("Create failure, table " + tableName + " exits.");
			logger.info("Create failure, table " + tableName + " exits.");
		}else{
			admin.createTable(table);
			System.out.println("Create success!");
			logger.info("Create success!");
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	private void cleanup(){
		if(conn != null){
			try {
				conn.close();
			} catch (IOException e) {
				optException.put("cleanup()", e.getMessage());
			}
		}
	}
	
	
	
}
