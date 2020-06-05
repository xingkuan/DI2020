package com.future.DI2020;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class JDBCtest {
    private final String url = "jdbc:postgresql://dbatool01:5432/didb";
    private final String user = "repuser";
    private final String password = "passwd";

    //private static final MetaData metaData = MetaData.getInstance();

    public Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return conn;
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        JDBCtest app = new JDBCtest();
        app.connect();
    }
}