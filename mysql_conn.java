import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static java.sql.DriverManager.getConnection;

public class MySqlApp {
    public static void main(String[] args) {
        System.out.println("start MySqlApp");




        String url = "jdbc:mysql://***.***.***.**:****";
        String username = "dk";
        String password = "password";

        
        String sqlCommand = "INSERT INTO daria_db.dk_test (a) VALUES (77);";
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").getDeclaredConstructor().newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        try {
            Connection conn = DriverManager.getConnection("jdbc:mysql://***.***.***.**:****", "dk", "****");
            Statement st = conn.createStatement();
            st.execute(sqlCommand);

            ResultSet rs = st.executeQuery("select * from DARIA_DB.dk_test;");
            //System.out.println(st.executeQuery(sqlCommand));

            System.out.println("**************Result**************");

            while ( rs.next() ) {
                String all = rs.getString("a");
                System.out.println(all);
            }


            // creating table
            //for (int i=0; i<11; i++ ) {
               //statement.executeUpdate(sqlCommand + i + "','2')  ;");
           //}
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("stop MySqlApp");
    }
}
