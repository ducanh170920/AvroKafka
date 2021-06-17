package week3;

import org.fluttercode.datafactory.impl.DataFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static java.sql.DriverManager.getConnection;

public class DataToSQL {
    private static String DB_URL = "jdbc:mysql://localhost:3306/packages";
    private static String USER_NAME = "root";
    private static String PASSWORD = "01676607122";
    public static void main(String[] args) throws IOException, SQLException {

        DataFactory dataFactory = new DataFactory();
        Connection conn = getConnection(DB_URL, USER_NAME, PASSWORD);;
        Statement stmt = conn.createStatement();

        String query = "INSERT INTO customers_packages(shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created" + ","+
                " pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, created, modified, is_cancel, ightk_user_id) VALUES";

        BufferedReader reader = new BufferedReader(new FileReader("./data.csv"));

        long n = 5000000;
        int BATCH_SIZE = 10000;
        int index = 0;
        reader.readLine();
        reader.readLine();
        String line;
        int count = 0;
        long timeStart = System.currentTimeMillis();

        while ((line = reader.readLine())!=null) {
            query += line;
            index++;
            if(index ==  BATCH_SIZE - 1 + count * BATCH_SIZE){
                query+=';';
                System.out.println("Running Time:" + (System.currentTimeMillis()-timeStart));
                stmt.executeUpdate(query);
                count++;
                query = "INSERT INTO customers_packages(shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created" + ","+
                        " pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, created, modified, is_cancel, ightk_user_id) VALUES";
            }else {
                query+=',';
            }

        }
        System.out.println("Running Time:" + (System.currentTimeMillis()-timeStart));
        reader.close();
        conn.close();

    }
}
