package com.experiment.hiveserver2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class HiveServerApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveLocalEmbeddedServer.class);
    private static boolean stopServerAtEOP = true;

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting Hive Server!");
        HiveLocalEmbeddedServer embeddedServer = new HiveLocalEmbeddedServer();
        Thread serverThread = new Thread(embeddedServer);
        serverThread.start();
        while(embeddedServer == null || !embeddedServer.isStarted()) {
            Thread.sleep(1000L);
        }
        LOGGER.info("Starting Hive Server... Done!");
        Integer port = embeddedServer.getPort();

        Connection connection = null;
        Statement stmt = null;
        try {
            connection = DriverManager.getConnection("jdbc:hive2://localhost:" + port + "/default", "", "");
            stmt = connection.createStatement();
            LOGGER.info("Creating Connection... Done!");

            executeUpdate(stmt, "drop table if exists sample");
            executeUpdate(stmt, "create table sample(id int)");
            LOGGER.info("Creating Sample Table... Done!");

            executeUpdate(stmt, "insert into emp values(1)");
            executeUpdate(stmt, "insert into emp values(2),(3)");
            LOGGER.info("Insertion to Sample Table... Done!");

            ResultSet resultSet = executeSelect(stmt, "select * from sample");
            while (resultSet.next()) {
                LOGGER.info("DB 'Sample' Table Entry: " + resultSet.getInt("id"));
            }
            LOGGER.info("Querying Sample Table... Done!");
        } catch (Exception e) {
            stopServerAtEOP = true;
        } finally {
            if(stmt != null) {
                stmt.close();
            }
            if(connection != null) {
                connection.close();
                LOGGER.info("Connection closed!");
            }
            if(embeddedServer.isStarted() && stopServerAtEOP) {
                embeddedServer.stop();
            }
        }
    }

    private static void executeUpdate(Statement stmt, String s) throws SQLException {
        stmt.execute(s);
        LOGGER.info("Query Executed: " + s);
    }

    private static ResultSet executeSelect(Statement stmt, String s) throws SQLException {
        stmt.execute(s);
        LOGGER.info("Query Executed: " + s);
        return stmt.getResultSet();
    }

}
