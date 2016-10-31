package learn.hbase.rudiment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static learn.hbase.utils.Utils.print;

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
	
	String tableName = "ln_test";
	String columnFamily = "colfamily_one";
	
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
	//@Test
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
	
	/**
	 * insert data into HBase table.
	 * table-rowkey-column family - column - value
	 * */
	@SuppressWarnings({ "deprecation", "resource" })
	//@Test
	public void insert() throws Exception{
		String rowKey1 = "user_id_zhangsan";
		columnFamily = "info";
		Put put = new Put(rowKey1.getBytes());
		//column family, column, value
		put.add(columnFamily.getBytes(), "name".getBytes(), "ZhangSan".getBytes());
	    put.add(columnFamily.getBytes(), "email".getBytes(), "zhangsan@gmail.com".getBytes());
	    put.add(columnFamily.getBytes(), "password".getBytes(), "zs1990..".getBytes());
	    
	    HTableInterface table = getTable("user");
	    table.put(put);
	    
	    //table.put(List<Put>); //批量插入
	}
	
	/**
	 * delete by row key.
	 * @throws Exception 
	 * */
	@SuppressWarnings("deprecation")
	//@Test
	public void delete() throws Exception{
		HTableInterface table = getTable("user");
		List<Delete> deleteRows = new ArrayList<Delete>();
		
		String rowKey = "user_id_zhangsan";
		Delete del = new Delete(rowKey.getBytes());
		deleteRows.add(del);
		
		//batch delete
		table.delete(deleteRows);
	}
	
	
	/**
	 * get data of row
	 * @throws Exception 
	 * */
	//@Test
	public void getOne() throws Exception{
		@SuppressWarnings("deprecation")
		HTableInterface table = getTable("user");
		String rowKey = "user_id_zhangsan";
		Get get = new Get(rowKey.getBytes());
		Result result = table.get(get);
		
		for(KeyValue kv : result.raw()){
			print("Row Key:" + new String(kv.getRow()));
			print("Column Family:" + new String(kv.getFamily()));
			print("Qualifier:" + new String(kv.getQualifier()));
			print("Timestamp:" + kv.getTimestamp());
			print("Value:" + new String(kv.getValue()));
		}
	}
	
	/**
	 * get all data.
	 * @throws Exception 
	 * */
	//@Test
	public void getAll() throws Exception{
		HTableInterface table = getTable("user");
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		
		for(Result result : scanner){
			for(KeyValue kv : result.raw()){
				print("Row Key:" + new String(kv.getRow()));
				print("Column Family:" + new String(kv.getFamily()));
				print("Qualifier:" + new String(kv.getQualifier()));
				print("Timestamp:" + kv.getTimestamp());
				print("Value:" + new String(kv.getValue()));
			}
		}
	}
	
	/**
	 * get part of data set
	 * @throws IOException 
	 * */
	@SuppressWarnings("deprecation")
	//@Test
	public void getApart() throws IOException{
		String rowKey1 = "user_id_zhangsan";
		columnFamily = "info";
		HTableInterface table = getTable("user");
		Get get = new Get(rowKey1.getBytes());
		//get.addFamily(columnFamily.getBytes());
		//column family, columnName
		get.addColumn(columnFamily.getBytes(), "email".getBytes());
	   
		Result result = table.get(get);
		
		for(KeyValue kv : result.raw()){
		    print(new String(kv.getValue()));
		}
	}
	
	
	/**
	 * get by timestamp
	 * @throws Exception 
	 * */
	@SuppressWarnings("deprecation")
	@Test
	public void getByTimeStamp() throws Exception{
		HTableInterface table = getTable("user");
		Get get = new Get("user_id_zhangsan".getBytes());
		//get.addFamily("info".getBytes());
		//get.addColumn("info".getBytes(), "email".getBytes());
	    Result result = table.get(get);
	    //result.getValue("info".getBytes(), "email".getBytes());
		
		List<KeyValue> kvs = result.getColumn("info".getBytes(), "email".getBytes());
	    kvs.forEach(item -> {
	    	System.out.println(new String(item.getValue()));
	    	System.out.println(item.getTimestamp());
	    });
	}
	
	
	
	private void cleanup(){
		if(!Objects.isNull(conn)){
			try {
				conn.close();
			} catch (IOException e) {
				optException.put("cleanup()", e.getMessage());
			}
		}
	}
	
	@SuppressWarnings({"deprecation", "resource" })
	private HTableInterface getTable(String tableName){
		 HTablePool tablePool = new HTablePool(configuration, 10);
		    HTableInterface table = tablePool.getTable(tableName.getBytes());
		 return table;   
	}
	
	
	
}
