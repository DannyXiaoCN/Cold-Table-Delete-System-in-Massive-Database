import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;


public class system_start {
    public static void main(String[] args) throws SQLException, IOException {
        new middle_ware(2, "query_generate2.txt", 100000, 600);
    }
}
