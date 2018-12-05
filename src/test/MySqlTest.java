import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by gaojian on 2018/9/12.
 */
public class MySqlTest {

    @Test
    public void test1(){
        String sql = "INSERT INTO NL_SVR_APACHE_MIN (`timestamp`,`server_instance_id`,`bps`,`qps`,`busy_workers`,`idle_workers`,`cpu`,`uptime`,`count`)  VALUES (2995584,1038,34,0,2,10,0,19246,2),(2995583,1038,17,0,1,5,0,9623,1)\n";

        Connection conn = getMysqlConn();
        try {
            executeSql(conn, sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Connection getMysqlConn() {
        // 不同的数据库有不同的驱动
        String driverName = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://dev-mysql-amb.tingyun.com:40000/lens_sys_data?allowMultiQueries=true";
        String user = "lens";
        String password = "nEtop2o10";

        Connection conn = null;
        try {
            // 加载驱动
            Class.forName(driverName);
            // 设置 配置数据
            // 1.url(数据看服务器的ip地址 数据库服务端口号 数据库实例)
            // 2.user
            // 3.password
            conn = DriverManager.getConnection(url, user, password);
//            conn.setAutoCommit(false);
            // 开始连接数据库
            System.out.println("数据库连接成功..");
        } catch (ClassNotFoundException e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        }

        return conn;
    }

    private void executeSql(Connection conn, String sql) throws SQLException {
        Statement statement = null;
        try {
            statement = conn.createStatement();
            statement.execute(sql);
        } finally {
            if(statement != null){
                statement.close();
            }
        }
    }


}
