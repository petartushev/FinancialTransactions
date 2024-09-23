package org.example.sinks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.configuration.Configuration;
import pojo.Transaction;


public class PostgresSink extends RichSinkFunction<Transaction> {

    private Connection connection;
    private PreparedStatement updateBalanceStatement;
    private PreparedStatement logTransactionStatement;

    private String jdbcUrl;
    private String username;
    private String password;

    public PostgresSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Establish connection to PostgreSQL
        connection = DriverManager.getConnection(jdbcUrl, username, password);


        String updateBalancesSql = "UPDATE postgres.public.account " +
                "SET balance = balance - ? " +
                "WHERE account_id = ?; " +
                "UPDATE postgres.public.account " +
                "SET balance = balance + ? " +
                "WHERE account_id = ?; ";

        String logTransactionSql = "INSERT INTO postgres.public.transactions (transaction_id, from_account, to_account, amount) " +
                "VALUES(?, ?, ?, ?); ";

        updateBalanceStatement = connection.prepareStatement(updateBalancesSql);
        logTransactionStatement = connection.prepareStatement(logTransactionSql);
    }

    @Override
    public void invoke(Transaction transaction, Context context) throws SQLException {
        // Set the values for the SQL insert statement
        updateBalanceStatement.setDouble(1, transaction.getAmount());
        updateBalanceStatement.setString(2, transaction.getSendingClientAccountNumber());
        updateBalanceStatement.setDouble(3, transaction.getAmount());
        updateBalanceStatement.setString(4, transaction.getReceivingClientAccountNumber());

        logTransactionStatement.setString(1, transaction.getTransactionId());
        logTransactionStatement.setString(2, transaction.getSendingClientAccountNumber());
        logTransactionStatement.setString(3, transaction.getReceivingClientAccountNumber());
        logTransactionStatement.setDouble(4, transaction.getAmount());
        // Execute the SQL statement

        try{
            updateBalanceStatement.executeUpdate();
            logTransactionStatement.executeUpdate();
        }
        catch (Exception e) {
            System.out.println(transaction);
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
        // Clean up the connection and prepared statement
        if (updateBalanceStatement != null) {
            updateBalanceStatement.close();
        }
        if (logTransactionStatement != null) {
            logTransactionStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}