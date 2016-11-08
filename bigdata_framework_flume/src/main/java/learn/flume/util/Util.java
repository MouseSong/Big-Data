package learn.flume.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

public class Util {

	public static Properties getAvroProperties(){
		InputStream in = Util.class.getClassLoader().getResourceAsStream("avro_client.properties");
		try{
			Properties props = new Properties();
			props.load(in);
			return props;
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(Objects.nonNull(in)){
				try {
					in.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return null;
	}
	
	public static void main(String[] args) {
		System.out.println(Util.getAvroProperties());
	}
}
