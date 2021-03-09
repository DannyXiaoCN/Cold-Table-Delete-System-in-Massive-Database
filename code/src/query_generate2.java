import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class query_generate2 {

    public static void main(String[] args)  {
        HashMap<Integer, Integer> query_times = new HashMap<>();
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("query_generate2.txt"));
            Random r = new Random();
            for (int i = 0; i <= 100000; i++) {
                int cur_num = r.nextInt(150);
                int cur_id = r.nextInt(100);
                bw.write("SELECT content1 FROM test"+(cur_num)+" WHERE id = "+cur_id+";\n");
                if (query_times.containsKey(cur_num)) {
                    int ori = query_times.get(cur_num);
                    query_times.put(cur_num, ori + 1);
                } else {
                    query_times.put(cur_num, 1);
                }
            }

            bw.close();
            System.out.println("文件创建成功！");
            BufferedWriter bw2 = new BufferedWriter(new FileWriter("distributed2.txt"));
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
