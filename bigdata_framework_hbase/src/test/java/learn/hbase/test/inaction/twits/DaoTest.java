package learn.hbase.test.inaction.twits;

import java.util.function.Supplier;

import org.junit.Test;

import learn.hbase.inaction.Infos;
import learn.hbase.inaction.dao.BaseDao;
import learn.hbase.inaction.dao.twits.TwitsDao;
import learn.hbase.inaction.dao.twits.impl.TwitsDaoImpl;

public class DaoTest {
	
	private BaseDao dao = new TwitsDaoImpl();

	//create table twits
	public static DaoTest create(Supplier<DaoTest> clazz){
		return clazz.get();
	}
	
	public void createTable() throws Exception{
	    dao.createTable(Infos.TABLE_TWITS, Infos.COLUMN_FAMILY_TWITS);
	}
	
	public void put(){
		TwitsDao tdao = (TwitsDao)dao;
		tdao.put("TheRealMT" + 1329088818321L, "twits", "dt", "1329088818321L");
		tdao.put("TheRealMT" + 1329088818321L, "twits", "twit", "Hello TwitBase");
		
	}
	
	public static void main(String[] args) {
		DaoTest test = DaoTest.create(DaoTest::new);
	    test.put();	
	}
	
}
