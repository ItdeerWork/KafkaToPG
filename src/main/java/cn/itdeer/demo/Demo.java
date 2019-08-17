package cn.itdeer.demo;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Description :
 * PackageName : cn.itdeer.demo
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/15/15:15
 */
public class Demo {

    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        Connection connection = DriverManager.getConnection("jdbc:postgresql://192.168.1.220:5432/demo", "demo", "12345678");
//        System.out.println(connection);
//
//
//        String[] hosts = "192.168.1.220,".split(",");
//        Properties properties = new Properties();
//
//        PGConnection bc = (PGConnection) connection;
////        CopyManager manager = new CopyManager(bc);
//
//
//        CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
//        copyManager.copyIn("");


        new Demo().copyFromFile(connection, "F:\\Code\\demo01.csv", "test");
    }


    public void copyFromFile(Connection connection, String filePath, String tableName)
            throws SQLException, IOException {


        String ss = "1027.HDGF,562,562.TGJ,1973,true,2019-08-15 13:24:28,2019-08-15 13:24:28\n367.HDGF,1896,1896.TGJ,420,true,2019-08-15 13:24:28,2019-08-15 13:24:28\n694.HDGF,1608,1608.TGJ,1622,true,2019-08-15 13:24:28,2019-08-15 13:24:28";
        InputStream is = new ByteArrayInputStream(ss.getBytes());

        FileInputStream fileInputStream = null;
        try {
            CopyManager copyManager = new CopyManager((BaseConnection) connection);
            fileInputStream = new FileInputStream(filePath);
            copyManager.copyIn("COPY " + tableName + " FROM STDIN USING DELIMITERS ','", new ByteArrayInputStream(ss.getBytes()));
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
