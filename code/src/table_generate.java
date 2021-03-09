import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class table_generate {
    private static final String URL = "jdbc:mysql://localhost:3306/BDA_EXPR";
    static final String NAME = "root";
    static final String PASSWORD = "242225Xbc";
    public static void main(String[] args)  {
        try {
            Connection conn = DriverManager.getConnection(URL, NAME, PASSWORD);
            Statement statement =conn.createStatement();
            for (int i = 0; i < 120; i++) {
                statement.executeUpdate("CREATE TABLE test"+ i +" (\n" +
                        "    id int,\n" +
                        "    content1 int,\n" +
                        "    content2 int,\n" +
                        "    content3 int,\n" +
                        "    primary key(id)\n" +
                        ");");
            }

            for (int i = 120; i < 140; i++) {
                statement.executeUpdate("CREATE TABLE test"+ i +" (\n" +
                        "    id int,\n" +
                        "    content1 int,\n" +
                        "    content2 int,\n" +
                        "    content3 int,\n" +
                        "    primary key(id),\n" +
                        "    CONSTRAINT fk_"+ i +" FOREIGN KEY(id) REFERENCES test"+ (i - 120) +" (id));");
            }

            for (int i = 140; i < 150; i++) {
                statement.executeUpdate("CREATE TABLE test" + i +" (\n" +
                        "    id int,\n" +
                        "    content1 int,\n" +
                        "    content2 int,\n" +
                        "    content3 int,\n" +
                        "    primary key(id),\n" +
                        "    CONSTRAINT fk_d_"+ i +" FOREIGN KEY(content1) REFERENCES test"+ (i - 50) +" (id),\n" +
                        "    CONSTRAINT fk_"+ i +" FOREIGN KEY(id) REFERENCES test"+ (i - 140) +" (id));");
            }
            System.out.println("表导入成功！");
        } catch (SQLException e) {
            //
        }
    }
}
