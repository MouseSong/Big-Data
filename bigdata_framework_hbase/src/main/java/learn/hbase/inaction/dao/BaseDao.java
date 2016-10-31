package learn.hbase.inaction.dao;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;

import learn.hbase.inaction.Infos;

public abstract class BaseDao{

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(BaseDao.class);
	private static Configuration configuration;
	private static Connection conn;
	
	//init configuration
	static{
		long start = System.currentTimeMillis();
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", Infos.ZK_SERVICE_PORT);
		configuration.set("hbase.zookeeper.quorum",Infos.ZK_NODES);
		configuration.set("hbase.master",Infos.HBASE_MASTER_ADDR);
	    try {
			conn = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			e.printStackTrace();
			logger.info("Create HBase Connection:" + e.getMessage());
		}
	    long end = System.currentTimeMillis();
	    logger.info("BaseDao->init: " + (end - start) + " millionseconds");
	}
	
	//get table instance of reference
	@SuppressWarnings("deprecation")
	protected HTableInterface getTable(String tableName){
		 HTablePool tablePool = new HTablePool(configuration, Infos.TABLE_POOL_CAPACITY);
		    HTableInterface table = tablePool.getTable(tableName.getBytes());
		 return table; 
	}
	
	//get hbase connection
	protected Connection getConnection(){
		if(!Objects.isNull(conn)){
			return conn;
		}else{
			throw new RuntimeException("Connection is null.");
		}
	}
	
	protected void cleanup(){
		if(Objects.nonNull(conn)){
			try {
				conn.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.info("BaseDao->cleanup: close connection failed.");
			}
		}
	}
	
	public void createTable(String tableName, String columnFamily) throws Exception {
		if(Objects.nonNull(tableName) && Objects.nonNull(columnFamily)){
			Connection conn  = getConnection();
		    HBaseAdmin admin = (HBaseAdmin)conn.getAdmin();
		    
		    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName.getBytes()));
		    tableDescriptor.addFamily(new HColumnDescriptor(columnFamily.getBytes()));
		    
		    //check table exist or not
		    if(admin.tableExists(tableName.getBytes())){
		    	logger.info("TwitsDaoImpl->createTable: table " + tableName + "has existed." );
		    }else{
		    	admin.createTable(tableDescriptor);
		    	logger.info("TwitsDaoImpl->createTable: create table " + tableName +" success." );
		    }
		    
		    cleanup();
       }
	}
}
