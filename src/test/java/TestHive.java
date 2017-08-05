import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;

/**
 * Created by Administrator on 2017/8/5.
 */
public class TestHive {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private Connection conn = null;
    private Statement stmt = null;

    @Before
    public void setUp() throws IOException {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection("jdbc:hive2://hadoop-senior.jason.com:10000/db_hive", "jason", "abc123");
            stmt = conn.createStatement();
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }catch (SQLException e){
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("Connection is set up");
    }

    @Test
    public void testSelectHive() throws IOException, SQLException {
        ResultSet resultSet = stmt.executeQuery("select * from bf_log_20170712");
        while(resultSet.next()){
            String ip = resultSet.getString(1);
            String user = resultSet.getString(2);
            String req_url = resultSet.getString(3);
            System.out.println("ip: "+ip+" user: "+user+" req_url: "+req_url);
        }
    }

    @After
    public void closeConn() throws SQLException {
        stmt.close();
        conn.close();
        System.out.println("Connection is closed");
    }

}
