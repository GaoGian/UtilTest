import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * 处理机器学习“指标异常预测”基础数据
 * Created by tingyun on 2018/7/25.
 */
public class BigDataTest {

    @Test
    public void test1(){
        String fileName = "AppInstanceAction";
        int startMetricIndex = 2;
        String startTime = "2018-08-21 00:00:00";
        int timeCount = 1440;

        String readFilePath = "C:\\Users\\tingyun\\Desktop\\临时\\BigData\\" + fileName + ".csv";
        String writeFilePath = "C:\\Users\\tingyun\\Desktop\\临时\\BigData\\BigDataBaseResult_" + fileName + "_temp.csv";

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        List<String> timeStamps = new ArrayList<>();
        for(int i = 0; i < timeCount; i++){
            Date shardTime = null;
            try {
                shardTime = sdf.parse(startTime);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            String timeStr = sdf.format(shardTime.getTime() + 60 * i * 1000);
            timeStamps.add(timeStr);
        }

        FileReader reader = null;
        try {
            reader = new FileReader(readFilePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Set<String> appKeys = new HashSet<>();
        String[] headers = null;
        BufferedReader br = new BufferedReader(reader);
        Integer fieldCount = 0;

        Connection conn = getMysqlConn();
        StringBuilder sql = new StringBuilder();
        try {
            int count = 0;
            int fieldNum = 0;
            String line = null;
            while ((line = br.readLine()) != null) {
                if(count == 0) {
                    headers = line.split(",");
                }else{
                    String[] metricValues = line.split(",");
                    String timeStamp = metricValues[0];
                    String appKey = metricValues[1];
                    appKeys.add(appKey);

                    String tableName = getTableName(timeStamp);

                    for (int i = startMetricIndex; i < headers.length; i++) {
                        String field = headers[i];
                        String metricValue = "NULL".equals(metricValues[i]) ? "0" : metricValues[i];
                        sql.append("update " + tableName + " set `" + timeStamp + "` = '" + metricValue + "' where headerKey = '" + appKey + "|" + field + "';") ;

                    }

                    fieldNum += metricValues.length - 2;

                }

                count++;

                if(count % 500 == 0) {
                    boolean complete = false;
                    System.out.println("read count: " + count);
                    System.out.println("app start commit sql, time: " + sdf.format(System.currentTimeMillis()));
                    while (!complete) {
                        try {
                            executeSql(conn, sql.toString());
                            complete = true;
                            sql = new StringBuilder();
                        } catch (Throwable e) {
                            e.printStackTrace();

                            Thread.sleep(10000L);

                            conn = getMysqlConn();
                        }
                    }
                    System.out.println("app commit sql complete, time: " + sdf.format(System.currentTimeMillis()));
                }

            }

            System.out.println("data count: " + count + ", fieldNum: " + fieldNum);
        } catch (Exception e) {
            try {
                br.close();
                reader.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }

//        FileWriter writer = null;
//        try {
//            writer = new FileWriter(writeFilePath);
//            writeFileMetricValue("headerKey", "", writer);
//            for (String timeStamp : timeStamps) {
//                writeFileMetricValue(timeStamp, ",", writer);
//            }
//            writeFileMetricValue("\r\n", "", writer);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
////        outputFile(result, writer, timeStamps, fieldCount);
//        outputFileFromRedis(appKeys, timeStamps, headers, startMetricIndex, pipeline, writer, fieldCount);
//        System.out.println("final fieldCount: " + fieldCount);
//
//        if(writer != null){
//            try {
//                writer.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }

    }

    private Pipeline initRedisPipline(){
        JedisPoolRedisClientImpl redisClient = new JedisPoolRedisClientImpl();
        GenericObjectPoolConfig poolConfig = new JedisPoolConfig();
        redisClient.setHost("dev-redis.tingyun.com");
        redisClient.setPassword("nbs!@#123");
        redisClient.setPoolConfig(poolConfig);
        redisClient.init();
        Pipeline pipeline = redisClient.pipelined();
        return pipeline;
    }

    private Connection getMysqlConn() {
        // 不同的数据库有不同的驱动
        String driverName = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://dev-mysql-conf.tingyun.com:3306/test?allowMultiQueries=true";
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

    private String getTableName(String timeStamp){
        if(timeStamp.compareTo("2018-08-21 07:00:00") < 0){
            return "bigdata_test_1";
        }else if(timeStamp.compareTo("2018-08-21 12:00:00") < 0){
            return "bigdata_test_2";
        }else if(timeStamp.compareTo("2018-08-21 18:00:00") < 0){
            return "bigdata_test_3";
        }else {
            return "bigdata_test_4";
        }
    }

//    private void outputFileFromMysql(Set<String> appKeys, String[] headers, int startMetricIndex, Connection connection, FileWriter writer, Integer fieldCount){
//        boolean complete = false;
//        String sql = "select * from ? limit ?, ?";
//        int tableShardIndex = 1;
//        int count = 0;
//        while(!complete){
//            try {
//                PreparedStatement preparedStatement = connection.prepareStatement(sql);
//                preparedStatement.setString(1, "bigdata_test_" + tableShardIndex);
//                preparedStatement.setInt(2, count);
//                preparedStatement.setInt(3, count + 5000);
//
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//
//        }
//    }



    private void outputFileFromRedis(Set<String> appKeys, List<String> timeStamps, String[] headers, int startMetricIndex,
                                     Pipeline pipeline, FileWriter writer, Integer fieldCount){

        Map<String, String> metricValues = pipeline.hgetAll("BIG_DATA").get();

        int fieldNum = 0;
        for(String appKey : appKeys) {
            for (int i = startMetricIndex; i < headers.length; i++) {
                try {
                    writeFileMetricHeader(appKey + "|" + headers[i] + "\t", writer);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                int num = 0;
                for(String timeStamp : timeStamps) {
                    String field = headers[i];
                    String metricValue = metricValues.get(field + ":" + timeStamp.replace(":", "-").replace(" ", "_") + ":" + appKey);

                    if(metricValue == null){
                        metricValue = "0";
                    }

                    if (num == 0) {
                        try {
                            writeFileMetricValue(metricValue, "", writer);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        try {
                            writeFileMetricValue(metricValue, ",", writer);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    num++;
                    fieldCount++;

                }
                fieldNum++;
                try {
                    writeFileMetricValue("\r\n", "", writer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("write fieldNum: " + fieldNum);
        }

    }

    private void outputFile(Map<String, Map<String, Map<String, String>>> result, FileWriter writer, List<String> timeStamps, Integer fieldCount){
        for(String appKey : result.keySet()){
            Map<String, Map<String, String>> appResultMap = result.get(appKey);

            for(String metricKey : appResultMap.keySet()) {
                try {
                    Map<String, String> metricMap = appResultMap.get(metricKey);
                    writeFileMetricHeader(appKey + "|" + metricKey + "\t", writer);
                    int num = 0;
                    for (String timeStamp : timeStamps) {
                        String metricValue = metricMap.get(timeStamp) == null ? "0" : metricMap.get(timeStamp);

                        if (num == 0) {
                            writeFileMetricValue(metricValue, "", writer);
                        } else {
                            writeFileMetricValue(metricValue, ",", writer);
                        }

                        num++;
                        fieldCount++;
                    }
                    writeFileMetricValue("\r\n", "", writer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private static void writeFileMetricHeader(String header, FileWriter writer) throws IOException {
        writer.write(header);
    }

    private static void writeFileMetricValue(String value, String separator, FileWriter writer) throws IOException {
        writer.write(separator + value);
    }

}
