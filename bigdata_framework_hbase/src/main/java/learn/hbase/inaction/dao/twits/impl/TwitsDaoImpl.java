package learn.hbase.inaction.dao.twits.impl;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import learn.hbase.inaction.Infos;
import learn.hbase.inaction.dao.BaseDao;
import learn.hbase.inaction.dao.twits.TwitsDao;

public class TwitsDaoImpl extends BaseDao implements TwitsDao{
	
	private static Logger logger = LoggerFactory.getLogger(TwitsDaoImpl.class);

	@Override
	public void put(String rowKey, String clumnFamily, String qualifier, String value) {
		// TODO Auto-generated method stub
		HTableInterface table = getTable(Infos.TABLE_TWITS);
		Put put = new Put(rowKey.getBytes());
		put.addColumn(clumnFamily.getBytes(), qualifier.getBytes(), value.getBytes());
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
			logger.info("TwitsDaoImpl->put: put data failed," + e.getMessage());
		}
	}

	
}
