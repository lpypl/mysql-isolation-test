import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

public class Main {
    private final static String jdbcUri = "jdbc:mysql://localhost:3306?serverTimezone=UTC&useServerPrepStmts=true&cachePrepStmts=true";
    private final static String jdbcUriDb0 = "jdbc:mysql://localhost:3306/db0?serverTimezone=UTC&useServerPrepStmts=true&cachePrepStmts=true";
    private final static String username = "root";
    private final static String password = "root";

    public static void main(String[] args) throws SQLException, IOException {
        loadSchema();
        loadInitData();

        Thread t1 = new Thread(() -> {
            try {
                loadTransaction1();
            } catch (SQLException | InterruptedException exception) {
                exception.printStackTrace();
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                loadTransaction2();
            } catch (SQLException | InterruptedException exception) {
                exception.printStackTrace();
            }
        });

        t1.start();
        t2.start();
    }

    public static void loadSchema() throws SQLException, IOException {
        // CREATE DATABASE
        Connection conn = getConnection(false);
        Statement stat = conn.createStatement();
        stat.execute("DROP DATABASE IF EXISTS db0;");
        stat.execute("CREATE DATABASE db0;");
        stat.close();

        conn = getConnection(true);
        stat = conn.createStatement();

        BufferedReader reader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(ClassLoader.getSystemResourceAsStream("schema.sql"))));
        String str;
        while ((str = reader.readLine()) != null) {
            stat.addBatch(str);
        }
        stat.executeBatch();
        stat.close();
    }

    public static void loadInitData() throws SQLException, IOException {
        Connection conn = getConnection(true);
        Statement stat = conn.createStatement();
        BufferedReader reader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(ClassLoader.getSystemResourceAsStream("dataInsert.sql"))));
        String str;
        while ((str = reader.readLine()) != null) {
            stat.addBatch(str);
        }
        stat.executeBatch();
        stat.close();
    }

    public static void loadTransaction1() throws SQLException, InterruptedException {
        Connection conn = getConnection(true);
        conn.setAutoCommit(false);

        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        Statement stat = conn.createStatement();
        System.out.printf("[ %d ]SELECT will start...\n", System.currentTimeMillis());
        stat.executeQuery("select pkAttr0, pkAttr1 from table0  order by pkAttr0;");
        stat.close();
        System.out.printf("[ %d ]SELECT finished...\n", System.currentTimeMillis());

        System.out.printf("[ %d ]TRANSACTION-1 waiting for 30s...\n", System.currentTimeMillis());
        Thread.sleep(30000);
        System.out.printf("[ %d ]TRANSACTION-1 waiting for 30s finished...\n", System.currentTimeMillis());

        conn.commit();
        System.out.printf("[ %d ]TRANSACTION-1 committed...\n", System.currentTimeMillis());
    }

    public static void loadTransaction2() throws SQLException, InterruptedException {
        Connection conn = getConnection(true);
        conn.setAutoCommit(false);

        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        System.out.printf("[ %d ]TRANSACTION-2 waiting for 5s...\n", System.currentTimeMillis());
        Thread.sleep(5000);
        System.out.printf("[ %d ]TRANSACTION-2 waiting for 5s finished...\n", System.currentTimeMillis());

        Statement stat = conn.createStatement();

        System.out.printf("[ %d ]INSERT will start...\n", System.currentTimeMillis());
        stat.executeUpdate("insert into table0(pkId, pkAttr0, pkAttr1, coAttr0, coAttr1, coAttr2) " +
                "values(14, 142, 'vc141', '52319H0Evzo42wRmp93zh7VHjdZG3y0', 65701, '11963l4Ts3sPRfQbzleHx');");
        System.out.printf("[ %d ]INSERT finished...\n", System.currentTimeMillis());
        stat.close();
        conn.commit();
        System.out.printf("[ %d ]TRANSACTION-2 committed...\n", System.currentTimeMillis());
    }

    public static Connection getConnection(boolean withDB) throws SQLException {

        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        if (withDB) {
            return DriverManager.getConnection(jdbcUriDb0, username, password);
        } else {
            return DriverManager.getConnection(jdbcUri, username, password);
        }
    }
}
