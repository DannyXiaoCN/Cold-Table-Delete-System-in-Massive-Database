import java.io.*;
import java.sql.*;
import java.util.*;

public class middle_ware {


    // 连接数据库BDA_EXPR
    private static final String URL = "jdbc:mysql://localhost:3306/BDA_EXPR";
    static final String NAME = "root";
    static final String PASSWORD = "242225Xbc";
    Connection conn;

    /* 创建系统需要的一些数组
     * each_query_num 为存储每个表与其对应被操作的次数
     * table_info 存储每个表的外键信息，包括外键名和约束表
     * deleted_table 存储已经被删除的表
     * query_sum 记录总操作数 */
    Integer query_sum = 0;
    HashMap<String, Integer> each_query_num = new HashMap<>();
    HashMap<String, table_name_foreign_key> table_info = new get_init_data().Output();
    List<String> deleted_table = new ArrayList<>();
    int ct_sh;




    /* code: 输入模式，1为命令行实时输入模式，2为文件输入模式
     * addr: 若为文件模式，填写文件的绝对地址，若不是，输入空格即可
     * delete_start_sh: 对数据库进行冷热表筛选操作判定的阙值
     * sh: 冷表判定阙值 */
    public middle_ware(int code, String addr, int delete_start_sh, int sh) throws SQLException, IOException {

        long startTime = System.currentTimeMillis();
        ct_sh = sh;
        try {
            conn = DriverManager.getConnection(URL, NAME, PASSWORD);
            System.out.println("获取数据库链接成功");
        } catch (SQLException e) {
            System.out.println("获取数据库连接失败");
            e.printStackTrace();
        }
        init();
        Scanner sc;
        if (code == 1) {
            sc = new Scanner(System.in);
        } else {
            sc = new Scanner(new File(addr));
        }
        System.out.println("请输入标准SQL操作");
        System.out.println("请输入单层语句，本系统目前不支持嵌套语句");
        while (sc.hasNext()) {
            String input = sc.nextLine();
            if (input.equals("quit")) {
                System.out.println("系统已关闭");
                return;
            }
            read_in(input);
            deletion(delete_start_sh, sh);
        }
        long endTime = System.currentTimeMillis();
        BufferedWriter bw2 = new BufferedWriter(new FileWriter("timer.txt"));
        bw2.write("总程序运行时间：" + (endTime - startTime) + "ms\n");
        bw2.close();
    }

    /* 初始化
     * 首先获取所有表名，将each_query_num初始化为表名与0 */
    public void init() throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet rs_i = statement.executeQuery("show tables;");
        while (rs_i.next()) {
            String table_name = rs_i.getString("Tables_in_bda_expr");
            each_query_num.put(table_name, 0);
        }
    }

    /* 该函数接受输入的表名，输出为-1时表示该表不存在，输出为0时表示该表存在并完成对该表的数据更新 */
    public int analyze(String tab_name) throws SQLException {
        // 该表存在于deleted_table中表示该表已经被删除，返回-1
        if (deleted_table.contains(tab_name)) {
            System.out.println("此表为空、不存在或者已经被删除");
            return -1;
        }
        // 若不存在该表信息，表示该表没有正确导入，表示其不存在，返回-1
        // 若正确存在信息，更新query_sum变量加1，更新each_query_num中对应该表的位置加1
        if (each_query_num.containsKey(tab_name)) {
            query_sum++;
            Integer cur_query_num = each_query_num.get(tab_name);
            cur_query_num += 1;
            each_query_num.put(tab_name, cur_query_num);
        } else {
            System.out.println("此表为空、不存在或者已经被删除");
            return -1;
        }
        return 0;
    }

    /* 该函数接受输入的表名，输出该表具有的所有列名 */
    public List<String> get_name(String tab_name) throws SQLException {
        List<String> result = new LinkedList<String>();
        Statement statement_g = conn.createStatement();
        ResultSet rs_g = statement_g.executeQuery("DESCRIBE " + tab_name + ";");
        while (rs_g.next()){
            result.add(rs_g.getString(1));
        }
        return result;
    }

    /* 读入处理部分
     * 将输入的sql语句进行处理，使之被系统记录，并向Mysql数据库进行操作获取结果并返回
     * 其中输入的sql语句不允许嵌套的形式名，且操作名必须均为大写
     * 根据输入的sql语句分为SELECT, DELETE, UPDATE, INSERT
     * 每当有一个合法的输入，都会通过analyze函数判断该表是否已经删除，若没有删除，进行计数，
     * 若对象表不存在，则会返回The table is not existed or already deleted because it is rarely queried/used
     * 当其为SELECT，首先判断是否明确了需要的列名，若没有提供列名，需要利用get_name函数对'*'所对应的列名进行查找
     * 并根据查找的结果进行查询
     * 当其DELETE, UPDATE, INSERT时，系统的处理差不多，若输入合法且对象表存在，则传递至数据库
     * 除了这四种操作外，其他操作均为不合法操作，会返回Not supported currently */
    public void read_in(String operating_sql) throws SQLException {
        Statement statement = conn.createStatement();
        String[] sql = operating_sql.split(" ");
        String cur_op = sql[0];
        String tab_name = null;
        // 输入为SELECT时
        if (cur_op.equals("SELECT")) {
            for (int i = 0; i < sql.length; i++) {
                if (sql[i].equals("FROM")) {
                    if (i + 2 == sql.length) {
                        tab_name = sql[i + 1].substring(0, sql[i + 1].length() - 1);
                        break;
                    } else {
                        tab_name = sql[i + 1];
                    }
                }
            }
            // 判断对象表是否已被删除
            int if_delete = analyze(tab_name);
            if (if_delete == -1) {
                System.out.println("The table is not existed or already deleted because it is rarely queried/used");
                return ;
            }
            // 执行SQL语句
            ResultSet rs = statement.executeQuery(operating_sql);
            // 若无明确查询列名（全选择），根据get_name的返回结果来输出列名+查询结果
            // 输出格式为 列名 列名 列名 ...
            // 输出格式为 结果 结果 结果 ...
            if (sql[1].equals("*")) {
                List<String> temp = get_name(tab_name);
                for (String s : temp) {
                    System.out.print(s + ' ');
                }
                System.out.println();
                while (rs.next()) {
                    List<String> temp_str = new LinkedList<>();
                    for (String s : temp) {
                        temp_str.add(rs.getString(s));
                    }

                    for (String s : temp_str) {
                        System.out.print(s + ' ');
                    }
                    System.out.println();
                }
            } else {
                // 给出查询列名，直接返回列名+查询结果
                while (rs.next()) {
                    List<String> temp_str = new LinkedList<>();
                    for (String s : sql[1].split(",")) {
                        temp_str.add(rs.getString(s));
                        System.out.print(s + ' ');
                    }
                    System.out.println();
                    for (String s : temp_str) {
                        System.out.print(s + ' ');
                    }
                    System.out.println();
                }
            }
        // 以下三种操作不会有返回结果
        // 当sql语句执行成功时，输出Success，若对象表不存在则输出The table is not existed or already deleted because it is rarely queried/used
        } else if (cur_op.equals("DELETE") && sql[1].equals("FROM")) {
            tab_name = sql[2];
            int if_delete = analyze(tab_name);
            if (if_delete == -1) {
                System.out.println("The table is not existed or already deleted because it is rarely queried/used");
                return ;
            }
            int s = statement.executeUpdate(operating_sql);
            if (s > 0) {
                System.out.println("Success");
            } else {
                System.out.println("The table is not existed or already deleted because it is rarely queried/used");
            }
        } else if (cur_op.equals("UPDATE")) {
            tab_name = sql[1];
            int if_delete = analyze(tab_name);
            if (if_delete == -1) {
                System.out.println("The table is not existed or already deleted because it is rarely queried/used");
                return ;
            }
            int s = statement.executeUpdate(operating_sql);
            if (s > 0) {
                System.out.println("Success");
            } else {
                System.out.println("The table is not existed or already deleted because it is rarely queried/used");
            }
        } else if (cur_op.equals("INSERT") && sql[1].equals("INTO")) {
            tab_name = sql[2];
            int if_delete = analyze(tab_name);
            if (if_delete == -1) {
                System.out.println("The table is not existed or already deleted because it is rarely queried/used");
                return ;
            }
            int s = statement.executeUpdate(operating_sql);
            if (s > 0) {
                System.out.println("Success");
            } else {
                System.out.println("The table is not existed or already deleted because it is rarely queried/used");
            }
        // 除上述4种操作以外的操作不支持
        } else {
            System.out.println("Not supported currently");
        }
    }


    /* 删除部件
     * 输入为用户输入系统的删除阙值与冷表阙值，函数作用为更新数据库中的表
     * 去除冷表，保留热表
     * 若一个表为冷表，且无外键约束，将其直接删除，并在delete_table上进行标记
     * 其中如果冷表存在热表对其的外键约束
     * 若其外键约束表中存在热表
     * 则只保留其主键列，删除其他列（外键约束只对主键有作用），在delete_table上进行标记
     * 若其外键约束表全部为冷表
     * 则去除所有外键信息并完成删除，在delete_table上进行标记
     * 完成删除操作后，将query_sum变量清零，ct_sh叠加 */
    public void deletion(int delete_start_sh, int sh) throws SQLException {

        // 判断总操作数是否达到删除阙值
        if (query_sum < delete_start_sh) {
            return;
        }

        // 将每个表的信息获取，开始循环
        for (Map.Entry<String, Integer> entry: each_query_num.entrySet()) {
            // 如果该表已被删除，跳过
            if (deleted_table.contains(entry.getKey())) {
                continue;
            }
            Integer cur_num = entry.getValue();
            String cur_tab = entry.getKey();
            // leij变量在该表存在一个外键约束热表时加一，如果它最终为0，则代表其所有外键约束表都为冷表
            // 则该表可以去除所有外键信息并完成删除
            int leij = 0;
            // 表的操作数低于冷表阙值，被判定为冷表
            if (cur_num <= ct_sh) {
                // 判断是否具有外键约束
                if (table_info.get(cur_tab).foreignkey_id.isEmpty()) {
                    // 无外键约束，则直接删除，并在delete_table上进行标记
                    Statement statement_temp = conn.createStatement();
                    statement_temp.executeUpdate("DROP TABLE " + cur_tab +";");
                    System.out.println(cur_tab +" has been deleted");
                    deleted_table.add(cur_tab);
                } else {
                    // 冷表存在外键约束
                    // 判断其外键约束表是否存在热表
                    table_name_foreign_key temp = table_info.get(cur_tab);
                    for (String each_table_name: temp.referenced_table){
                        if (each_query_num.get(each_table_name) > sh) {
                            leij += 1;
                        }
                    }
                    // 判断其外键约束表是否存在热表
                    if (leij == 0) {
                        // 无热表存在，去除所有外键信息并完成删除，在delete_table上进行标记
                        Statement statement_temp = conn.createStatement();
                        for (int j = 0; j < temp.referenced_table.size(); j++) {
                            if (deleted_table.contains(temp.referenced_table.get(j))) {
                                continue;
                            }
                            // 去除所有外键信息
                            statement_temp.executeUpdate("alter table " + temp.referenced_table.get(j) +" " +
                                    "drop foreign key "+ temp.foreignkey_id.get(j) +";");
                        }
                        // 删除该表，在delete_table上进行标记
                        statement_temp.executeUpdate("DROP TABLE " + cur_tab +";");
                        System.out.println(cur_tab +" has been deleted");
                        deleted_table.add(cur_tab);
                    } else {
                        // 存在热表，只保留其主键列，删除其他列（外键约束只对主键有作用），在delete_table上进行标记
                        String cpk = table_info.get(cur_tab).primary_key;
                        List<String> columns = get_name(cur_tab);
                        for (String column: columns) {
                            if (!column.equals(cpk)) {
                                Statement statement_ed = conn.createStatement();
                                statement_ed.executeUpdate("ALTER TABLE " + cur_tab + " DROP COLUMN " + column + ";");
                                System.out.println(cur_tab +" has been deleted");
                                deleted_table.add(cur_tab);
                            }
                        }
                    }
                }
            }
        }
        // 将总数归零
        query_sum = 0;
        // 冷表阈值叠加
        ct_sh += sh;
    }
}
