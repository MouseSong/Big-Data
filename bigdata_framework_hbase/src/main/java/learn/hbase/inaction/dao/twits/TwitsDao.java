package learn.hbase.inaction.dao.twits;


public interface TwitsDao{

	void put(String rowKey, String clumnFamily, String qualifier, String value);
	
}
