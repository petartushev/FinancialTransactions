package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

public class FlinkIcebergDDL {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withBuiltInCatalogName("bank_transactions_catalog")
                .withBuiltInDatabaseName("financial_transactions")
                .build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

//        tableEnvironment.executeSql("USE CATALOG bank_transactions_catalog");
//        tableEnvironment.executeSql("USE DATABASE financial_transactions");
//        tableEnvironment.executeSql("DROP TABLE transactions");

        //        This sql creates a catalog, we need it only once
//        tableEnvironment.executeSql(
//                "CREATE CATALOG bank_transactions_catalog WITH (" +
//                        "  'type'='iceberg'," +
//                        "  'catalog-impl'='org.apache.iceberg.rest.RESTCatalog'," +
//                        "  'uri'='http://localhost:8181'," +
//                        "  'warehouse'='s3://warehouse/'," +
//                        "  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO'," +
//                        "  's3.endpoint'='http://localhost:9000'," +
//                        "  's3.access-key-id'='admin'," +
//                        "  's3.secret-access-key'='password'," +
//                        "  's3.path-style-access'='true'" +
//                        ")"
//        );

//        tableEnvironment.useCatalog("bank_transactions_catalog");
//        tableEnvironment.executeSql("CREATE DATABASE financial_transactions");


        tableEnvironment.executeSql(
                "CREATE TABLE transactions (" +
                        "transactionId STRING NOT NULL, " +
                        "sendingClientAccountNumber STRING NOT NULL, " +
                        "receivingClientAccountNumber STRING NOT NULL, " +
                        "amount FLOAT NOT NULL " +
                        ")" +
                        "WITH (" +
                        " 'connector' = 'iceberg', " +
                        " 'catalog-name' = 'bank_transactions_catalog', " +
                        " 'database-name' = 'financial_transactions', " +
                        " 'table-name' = 'transactions', " +
                        " 'format' = 'parquet', " +
                        " 'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO', " +
                        " 's3.endpoint' = 'http://localhost:9000', " +
                        " 's3.access-key-id' = 'admin', " +
                        " 's3.secret-access-key' = 'password', " +
                        " 's3.path-style-access' = 'true' " +
                        ")"
        );

        System.out.println(tableEnvironment.getCurrentCatalog());
        System.out.println(Arrays.toString(tableEnvironment.listDatabases()));
        System.out.println(Arrays.toString(tableEnvironment.listTables()));

        System.out.println(tableEnvironment.executeSql("SELECT * FROM bank_transactions_catalog.financial_transactions.transactions").collect());

//        try{
////            env.execute(FlinkIcebergDDL.class.getName());
//            tableEnvironment.exe
//        }
//        catch (Exception e){
//            e.printStackTrace();
//        }
    }
}
