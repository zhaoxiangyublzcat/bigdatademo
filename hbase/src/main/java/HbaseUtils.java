import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.ArrayList;

public class HbaseUtils {
	public static Configuration configuration = null;
	public static HBaseAdmin hBaseAdmin = null;
	public static HTable hTable = null;

	public HbaseUtils() {
		configuration = new Configuration();
		configuration.set("hbase.zookeeper.quorum", "master");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.master", "master:60000");
		try {
			hBaseAdmin = new HBaseAdmin(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 创建表
	 * @param tableName
	 * @param columnFamily
	 * @throws IOException
	 */
	public static void createTable(String tableName, ArrayList<String> columnFamily) throws IOException {
		hTable = new HTable(configuration, tableName);

	}

	/**
	 * 查询表
	 * @param tableName
	 */
	public static void getAllTable(String tableName){

	}

	/**
	 * 删除表
	 * @param tableName
	 */
	public static void deleteTable(String tableName){

	}

	public static void deleteOneRecord(){

	}
}
