import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class LinkHive {
	// hive驱动名称
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	// 链接hive2服务的地址
	private static String hive2Address = "jdbc:hive://master:10000/default";
	// 对hdfs有操作权利的用户
	private static String user = "hadoop";
	// hive密码
	private static String password = "";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		Connection con = DriverManager.getConnection(hive2Address, user, password);
		System.out.println(con);
	}
}
