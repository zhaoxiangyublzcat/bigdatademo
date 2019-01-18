import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseTest {
	public static Configuration configuration;

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "master");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.master", "master:60000");
	}

	public static void main(String[] args) throws IOException {
		//createTable("infomation");
		//listTable();
		//insertInfomation("infomation");
		QueryInfomation("infomation");
		//QueryColumn("infomation");
		//deleteData("infomation");

	}


	/**
	 * 创建表
	 *
	 * @param tableName
	 * @throws IOException
	 */
	public static void createTable(String tableName) throws IOException {
		// admin对象
		HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
		if (hBaseAdmin.tableExists(tableName)) {
			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.deleteTable(tableName);
		}

		HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
		// 添加column family
		hTableDescriptor.addFamily(new HColumnDescriptor("address"));
		hTableDescriptor.addFamily(new HColumnDescriptor("info"));

		hBaseAdmin.createTable(hTableDescriptor);
		hBaseAdmin.close();
	}

	/**
	 * 列出所有表
	 *
	 * @return
	 * @throws IOException
	 */
	public static List<String> listTable() throws IOException {
		ArrayList<String> tables = new ArrayList<String>();
		HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
		if (hBaseAdmin != null) {
			HTableDescriptor[] listTables = hBaseAdmin.listTables();
			if (listTables.length > 0) {
				for (HTableDescriptor tableDescriptor : listTables) {
					tables.add(tableDescriptor.getNameAsString());
					System.out.println(tableDescriptor.getNameAsString());
				}
			} else {
				System.out.println("HBase中为含有表");
			}
		}
		return tables;
	}

	/**
	 * 添加一组数据到指定表
	 *
	 * @param tableName
	 * @throws IOException
	 */
	public static void insertInfomation(String tableName) throws IOException {
		HTable table = new HTable(configuration, tableName);

		Put put = new Put(getBytes("master"));
		put.add(getBytes("address"), getBytes("country"), getBytes("China"));
		put.add(getBytes("address"), getBytes("province"), getBytes("beijing"));
		put.add(getBytes("address"), getBytes("city"), getBytes("beijing"));

		put.add(getBytes("info"), getBytes("age"), getBytes("28"));
		put.add(getBytes("info"), getBytes("brithday"), getBytes("1998-8-29"));
		put.add(getBytes("info"), getBytes("company"), getBytes("master"));

		table.put(put);
		table.close();
	}

	/**
	 * 查询表中的数据
	 *
	 * @param tableName
	 * @throws IOException
	 */
	public static void QueryInfomation(String tableName) throws IOException {
		HTable table = new HTable(configuration, tableName);
		// 根据rowKey查询
		Get get = new Get(getBytes("master"));
		Result result = table.get(get);
		System.out.println("rowKey" + new String(result.getRow()));
		for (KeyValue keyValue : result.raw()) {
			System.out.println("列簇：" + new String(keyValue.getFamily())
					+ "-列" + new String(keyValue.getQualifier())
					+ "-值" + new String(keyValue.getValue()));
		}

		table.close();
	}

	/**
	 * 查询表某一列的值
	 *
	 * @param tableName
	 * @throws IOException
	 */
	public static void QueryColumn(String tableName) throws IOException {
		HTable table = new HTable(configuration, tableName);
		Scan scan = new Scan();
		scan.addColumn(getBytes("info"), getBytes("company"));
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			System.out.println("rowKey==" + new String(result.getRow()));
			for (KeyValue keyValue : result.raw()) {
				System.out.println("列簇：" + new String(keyValue.getFamily())
						+ "=列：" + new String(keyValue.getQualifier())
						+ "-值：" + new String(keyValue.getValue()));
			}
		}
		resultScanner.close();
		table.close();
	}

	/**
	 * 删除一条数据
	 * @param tableName
	 * @throws IOException
	 */
	public static void deleteData(String tableName) throws IOException {
		HTable table = new HTable(configuration, tableName);

		Delete delete = new Delete(getBytes("master"));
		delete.deleteColumn(getBytes("info"),getBytes("age"));

		table.delete(delete);
		table.close();
	}


	public static byte[] getBytes(String string){
		if (string==null){
			string ="";
		}
		return Bytes.toBytes(string);
	}
}
