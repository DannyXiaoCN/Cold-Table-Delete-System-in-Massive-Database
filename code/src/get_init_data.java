import java.util.*;
import java.sql.*;

/*
 table_name_foreign_key类
 foreignkey_id 存储外键名
 referenced_table 约束表
 primary_key 主键名
 */
class table_name_foreign_key {
    List<String> foreignkey_id = new LinkedList<>();
    List<String> referenced_table = new LinkedList<>();
    String primary_key;
}

public class get_init_data {

    // 连接数据库
    private static final String URL = "jdbc:mysql://localhost:3306/BDA_EXPR";
    static final String NAME = "root";
    static final String PASSWORD = "242225Xbc";
    //用于传输，将一个表名与其外键信息形成映射
    HashMap<String, table_name_foreign_key> table_info = new HashMap<>();

    public HashMap<String, table_name_foreign_key> Output() throws SQLException {
        Connection conn = DriverManager.getConnection(URL, NAME, PASSWORD);
        Statement statement1 =conn.createStatement();
        Statement statement2 =conn.createStatement();
        // 获取所有表名
        ResultSet rs = statement1.executeQuery("show tables;");
        while (rs.next()) {
            String table_name = rs.getString("Tables_in_bda_expr");
            // 下面为获取外键操作
            table_name_foreign_key cur_table_info = new table_name_foreign_key();
            ResultSet rs_f = statement2.executeQuery("SELECT TABLE_NAME,CONSTRAINT_NAME,REFERENCED_COLUMN_NAME \n" +
                    "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE\n" +
                    "WHERE CONSTRAINT_SCHEMA ='BDA_EXPR' \n" +
                    "    AND REFERENCED_TABLE_NAME = '"+ table_name +"';");
            while (rs_f.next()) {
                // 获取索引表
                cur_table_info.referenced_table.add(rs_f.getString("TABLE_NAME"));
                // 获取外键名
                cur_table_info.foreignkey_id.add(rs_f.getString("CONSTRAINT_NAME"));
                Statement stm = conn.createStatement();
                // 下面为获取主键操作
                ResultSet pks = stm.executeQuery("SELECT k.column_name " +
                        "FROM information_schema.table_constraints t " +
                        "JOIN information_schema.key_column_usage k " +
                        "USING (constraint_name,table_schema,table_name) " +
                        "WHERE t.constraint_type='PRIMARY KEY' " +
                        "AND t.table_schema='BDA_EXPR' " +
                        "AND t.table_name= '" + table_name + "';");
                if (pks.next()) {
                    cur_table_info.primary_key = pks.getString("column_name");
                }
            }
            table_info.put(table_name, cur_table_info);
        }
        rs.close();
        statement1.close();
        statement2.close();
        conn.close();
        return table_info;
    }
}
