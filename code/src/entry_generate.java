import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class entry_generate {
    private static final String URL = "jdbc:mysql://localhost:3306/BDA_EXPR";
    static final String NAME = "root";
    static final String PASSWORD = "242225Xbc";
    public static void main(String[] args)  {
        try {
            Connection conn = DriverManager.getConnection(URL, NAME, PASSWORD);
            Statement statement =conn.createStatement();
            for (int l = 0; l < 140; l++) {
                for (int i = 1; i <= 2200; i++) {
                    statement.executeUpdate("INSERT INTO test"+ l +" \n" +
                            "(id, content1, content2, content3) \n" +
                            "VALUES \n" +
                            "("+ i +", "+ i +", "+ i%100 +", "+ i%10 +");");
                }
            }


            for (int l = 140; l < 150; l++) {
                for (int i = 1; i <= 100; i++) {
                    statement.executeUpdate("INSERT INTO test"+ l +" \n" +
                            "(id, content1, content2, content3) \n" +
                            "VALUES \n" +
                            "("+ i +", "+ i +", "+ i%100 +", "+ i%10 +");");
                }
            }
            System.out.println("数据导入成功！");
        } catch (SQLException e) {
            //
        }
    }
}
