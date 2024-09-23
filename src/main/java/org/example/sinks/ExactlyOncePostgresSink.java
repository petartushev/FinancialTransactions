package org.example.sinks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;


import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pojo.Transaction;
import serializers.NoOpConnectionSerializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;



public class ExactlyOncePostgresSink extends TwoPhaseCommitSinkFunction<Transaction, Connection, Void> {

    private static final Logger logger = LoggerFactory.getLogger(ExactlyOncePostgresSink.class);

    private Connection connection;

    private String addBalanceSql;
    private String substractBalanceSql;
    private String logTransactionSql;

    private String jdbcUrl;
    private String username;
    private String password;

    public ExactlyOncePostgresSink(String jdbcUrl, String username, String password) {
//        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
        super(new NoOpConnectionSerializer(), VoidSerializer.INSTANCE);
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;

        this.substractBalanceSql = "UPDATE postgres.public.account " +
                "SET balance = balance - ? " +
                "WHERE account_id = ?; ";

        this.addBalanceSql = "UPDATE postgres.public.account " +
                "SET balance = balance + ? " +
                "WHERE account_id = ?; ";

        this.logTransactionSql = "INSERT INTO postgres.public.transactions (transaction_id, from_account, to_account, amount) " +
                "VALUES(?, ?, ?, ?); ";
    }


    @Override
    protected void invoke(Connection connection, Transaction transaction, Context context) throws Exception {
        try {
            PreparedStatement addBalanceStatement = connection.prepareStatement(this.addBalanceSql);
            PreparedStatement subtractBalanceStatement = connection.prepareStatement(this.substractBalanceSql);
            PreparedStatement logTransactionStatement = connection.prepareStatement(this.logTransactionSql);

            // Update balances
            addBalanceStatement.setDouble(1, transaction.getAmount());
            addBalanceStatement.setString(2, transaction.getSendingClientAccountNumber());

            subtractBalanceStatement.setDouble(1, transaction.getAmount());
            subtractBalanceStatement.setString(2, transaction.getReceivingClientAccountNumber());

            // Log transaction
            logTransactionStatement.setString(1, transaction.getTransactionId());
            logTransactionStatement.setString(2, transaction.getSendingClientAccountNumber());
            logTransactionStatement.setString(3, transaction.getReceivingClientAccountNumber());
            logTransactionStatement.setDouble(4, transaction.getAmount());

            addBalanceStatement.executeUpdate();
            subtractBalanceStatement.execute();
            logTransactionStatement.executeUpdate();
            logger.info("Transaction processed: {}", transaction);
//            System.out.println("Transaction processed:" + transaction);
        }
        catch (Exception e) {
//            e.printStackTrace();
            logger.error("Error processing transaction: {}", transaction, e);
//            System.out.println("Error processing transaction: {}\", transaction");
            throw new RuntimeException(e);
        }

    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false);
    }

    @Override
    protected Connection beginTransaction() throws Exception {
        Connection conn = DriverManager.getConnection(this.jdbcUrl, this.username, this.password);
        conn.setAutoCommit(false);
        logger.info("Transaction started with a new database connection");
//        System.out.println("beginTransaction.");
        return conn;
    }

    @Override
    protected void preCommit(Connection connection) throws Exception {

    }

    @Override
    protected void commit(Connection connection) {
        try{
            connection.commit();
            logger.info("Transaction committed");
//            System.out.println("Transaction committed");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void abort(Connection connection) {
        try {
            connection.rollback();
//            System.out.println("Rollback.");
            logger.warn("Rollback.");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.close();
        logger.info("ExactlyOncePostgresSink closed");
//        System.out.println("ExactlyOncePostgresSink closed");
    }
}
