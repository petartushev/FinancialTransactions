package org.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;

import java.util.Arrays;

public class FlinkIcebergDDL {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //        This sql creates a catalog, we need it only once
        tableEnvironment.executeSql(
                "CREATE CATALOG bank_transactions_catalog WITH (" +
                        "  'type'='iceberg'," +
                        "  'catalog-impl'='org.apache.iceberg.rest.RESTCatalog'," +
                        "  'uri'='http://localhost:8181'," +
                        "  'warehouse.location'='s3://warehouse/'," +
                        "  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO'," +
                        "  's3.endpoint'='http://localhost:9000'," +
                        "  's3.access-key-id'='admin'," +
                        "  's3.secret-access-key'='password'," +
                        "  's3.path-style-access'='true'" +
                        ")"
        );

        tableEnvironment.useCatalog("bank_transactions_catalog");
        tableEnvironment.executeSql("CREATE DATABASE financial_transactions");


        tableEnvironment.executeSql(
                "CREATE TABLE transactions (" +
                        "transactionId STRING NOT NULL, " +
                        "sendingClientAccountNumber STRING NOT NULL, " +
                        "receivingClientAccountNumber STRING NOT NULL, " +
                        "amount DOUBLE NOT NULL " +
                        ")" +
                        "WITH (" +
                        " 'connector' = 'iceberg', " +
                        " 'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog', " +
                        " 'catalog-name' = 'bank_transactions_catalog', " +
                        " 'database-name' = 'financial_transactions', " +
                        " 'table-name' = 'transactions', " +
                        " 'format' = 'parquet', " +
                        " 'uri' = 'http://localhost:8181', " +
                        " 'warehouse' = 's3://warehouse/', " +
                        " 'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO', " +
                        " 'catalog-rest.endpoint' = 'http://localhost:8181', " +
                        " 's3.endpoint' = 'http://localhost:9000', " + // http://minio:9000
                        " 's3.access-key-id' = 'admin', " +
                        " 's3.secret-access-key' = 'password', " +
                        " 's3.path-style-access' = 'true' " +
                        ")"
        );

         /*
         The table will not be created, unless a terminal operation is performed.
         It is lazy loaded. With the SELECT operation below it will create the table.
         */
        tableEnvironment.executeSql("SELECT * FROM transactions");


        try{
            env.execute(FlinkIcebergDDL.class.getName());
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
