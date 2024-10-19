package org.example;

public class Variables {
    final static String PG_DB_USERNAME = "postgres";
    final static String PG_DB_PASSWORD = "postgres";
    final static String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";

    final static String KAFKA_BOOSTRAP_SERVERS = "localhost:9092";
    final static String KAFKA_TOPIC = "transactions";
    final static String KAFKA_GROUP_ID = "transactions_group";

    final static String CATALOG_NAME = "bank_transactions_catalog";
    final static String ICEBERG_DB_NAME = "financial_transactions";
    final static String ICEBERG_DB_TABLE_NAME = "transactions";

    final static String REST_CATALOG_URI = "http://localhost:8181";
    final static String S3_WAREHOUSE_LOCATION = "s3://warehouse/";
    final static String MINIO_API_ENDPOINT = "http://localhost:9000";
    final static String S3_ACCESS_KEY = "admin";
    final static String S3_SECRET_KEY = "password";
}
