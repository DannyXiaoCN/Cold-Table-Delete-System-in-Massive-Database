import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class query_generate {

    public static void main(String[] args)  {
        HashMap<Integer, Integer> query_times = new HashMap<>();
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("query_generate.txt"));
            Random r = new Random();
            for (int i = 0; i <= 20000; i++) {
                // 10w次查询
                // 40 - 50最安全
                int cur_num = r.nextInt(20);
                int cur_id = r.nextInt(1000);
                bw.write("SELECT * FROM test"+(cur_num)+" WHERE id = "+cur_id+";\n");
                if (query_times.containsKey(cur_num)) {
                    int ori = query_times.get(cur_num);
                    query_times.put(cur_num, ori + 1);
                } else {
                    query_times.put(cur_num, 1);
                }
            }

            for (int i = 0; i <= 10000; i++) {
                // 10w次查询
                // 40 - 50最安全
                int cur_num = r.nextInt(10);
                cur_num += 130;
                int cur_id = r.nextInt(1000);
                bw.write("SELECT content2 FROM test"+(cur_num)+" WHERE id = "+cur_id+";\n");
                if (query_times.containsKey(cur_num)) {
                    int ori = query_times.get(cur_num);
                    query_times.put(cur_num, ori + 1);
                } else {
                    query_times.put(cur_num, 1);
                }
            }

            for (int i = 0; i <= 10000; i++) {
                // 10w次查询
                // 40 - 50最安全
                int cur_num = r.nextInt(10);
                cur_num += 140;
                int cur_id = r.nextInt(1000);
                bw.write("SELECT * FROM test"+(cur_num)+" WHERE id = "+cur_id+";\n");
                if (query_times.containsKey(cur_num)) {
                    int ori = query_times.get(cur_num);
                    query_times.put(cur_num, ori + 1);
                } else {
                    query_times.put(cur_num, 1);
                }
            }

            for (int i = 0; i <= 10000; i++) {
                // 10w次查询
                // 40 - 50最安全
                int cur_num = r.nextInt(10);
                cur_num += 120;
                int cur_id = r.nextInt(1000);
                bw.write("SELECT content1 FROM test"+(cur_num)+" WHERE id = "+cur_id+";\n");
                if (query_times.containsKey(cur_num)) {
                    int ori = query_times.get(cur_num);
                    query_times.put(cur_num, ori + 1);
                } else {
                    query_times.put(cur_num, 1);
                }
            }

            for (int i = 0; i <= 20000; i++) {
                // 10w次查询
                // 40 - 50最安全
                int cur_num = r.nextInt(150);
                int cur_id = r.nextInt(1000);
                bw.write("SELECT id FROM test"+(cur_num)+" WHERE id = "+cur_id+";\n");
                if (query_times.containsKey(cur_num)) {
                    int ori = query_times.get(cur_num);
                    query_times.put(cur_num, ori + 1);
                } else {
                    query_times.put(cur_num, 1);
                }
            }

            // delete 操作
            for (int i = 0; i < 10; i++) {
                int cur_num = r.nextInt(10);
                cur_num += 40;
                int cur_id = r.nextInt(2000);
                bw.write("DELETE FROM test"+cur_num+" WHERE id ="+cur_id+";\n");
                if (query_times.containsKey(cur_num)) {
                    int ori = query_times.get(cur_num);
                    query_times.put(cur_num, ori + 1);
                } else {
                    query_times.put(cur_num, 1);
                }
            }

            // insert 操作
            for (int i = 0; i < 10; i++) {
                int cur_num = r.nextInt(10);
                cur_num += 40;
                int cur_id = r.nextInt(100);
                cur_id += 2500;
                bw.write("INSERT INTO test"+cur_num+" (id) VALUES ("+cur_id+");\n");
                if (query_times.containsKey(cur_num)) {
                    int ori = query_times.get(cur_num);
                    query_times.put(cur_num, ori + 1);
                } else {
                    query_times.put(cur_num, 1);
                }
            }

            // update 操作
            for (int i = 0; i < 10; i++) {
                int cur_num = r.nextInt(10);
                cur_num += 40;
                int cur_id = r.nextInt(2000);
                bw.write("UPDATE test"+cur_num+" SET content1 = 725 WHERE id = "+cur_id+";\n");
                if (query_times.containsKey(cur_num)) {
                    int ori = query_times.get(cur_num);
                    query_times.put(cur_num, ori + 1);
                } else {
                    query_times.put(cur_num, 1);
                }
            }

            bw.close();
            System.out.println("文件创建成功！");
            BufferedWriter bw2 = new BufferedWriter(new FileWriter("distributed.txt"));
            int sum = 0;
            for (Map.Entry<Integer, Integer> entry: query_times.entrySet()) {
                sum += entry.getValue();
            }
            for (Map.Entry<Integer, Integer> entry: query_times.entrySet()) {
                bw2.write("test"+entry.getKey()+" | "+entry.getValue()+ " | " + (entry.getValue() * 10000/sum) +"%%\n");
            }
            bw2.write("sum | "+sum);
            bw2.close();
        } catch (IOException e) {
            //
        }
    }
}

