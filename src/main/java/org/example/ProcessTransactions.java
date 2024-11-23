package org.example;

import com.google.gson.Gson;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.hadoop.conf.Configuration;
import pojo.Transaction;
import timestamp_utils.TransactionWatermarkStrategy;

import java.util.HashMap;
import java.util.Map;


public class ProcessTransactions {
    public static void main(String[] args) throws Exception {

        Dotenv dotenv = Dotenv.load();

        String AWS_ACCESS_KEY_ID = dotenv.get("AWS_ACCESS_KEY_ID");
        String AWS_SECRET_ACCESS_KEY = dotenv.get("AWS_SECRET_ACCESS_KEY");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Variables.KAFKA_BOOSTRAP_SERVERS)
                .setTopics(Variables.KAFKA_TOPIC)
                .setGroupId(Variables.KAFKA_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(100L);
        env.enableCheckpointing(2000);  // e.g., 60000 for every minute
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withBuiltInCatalogName(Variables.CATALOG_NAME)
                .withBuiltInDatabaseName(Variables.ICEBERG_DB_NAME)
                .build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        DataStream<String> dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka source");

        DataStream<Transaction> transactionDataStream = dataStream.map(value ->
                        new Gson().fromJson(value, Transaction.class))
                .assignTimestampsAndWatermarks(new TransactionWatermarkStrategy());


        TableSchema tableSchema = TableSchema.builder()
                .field("transactionId", DataTypes.STRING().notNull())
                .field("sendingClientAccountNumber", DataTypes.STRING().notNull())
                .field("receivingClientAccountNumber", DataTypes.STRING().notNull())
                .field("amount", DataTypes.DOUBLE().notNull())
                .build();

        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("uri", Variables.REST_CATALOG_URI);  // REST Catalog URI
        catalogProperties.put("warehouse", Variables.S3_WAREHOUSE_LOCATION);  // S3 warehouse location
        catalogProperties.put("io-impl", S3FileIO.class.getName());  // Use S3 File IO
        catalogProperties.put("s3.endpoint", Variables.MINIO_API_ENDPOINT);  // MinIO endpoint http://minio:9000
        catalogProperties.put("s3.access-key", AWS_ACCESS_KEY_ID);  // MinIO access key
        catalogProperties.put("s3.secret-key", AWS_SECRET_ACCESS_KEY);  // MinIO secret key
        catalogProperties.put("s3.path-style-access", "true");  // Required for MinIO

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID);
        hadoopConf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY);
        hadoopConf.set("fs.s3a.endpoint", Variables.MINIO_API_ENDPOINT);
        hadoopConf.set("fs.s3a.path.style.access", "true");

        CatalogLoader catalogLoader = CatalogLoader.custom(
                Variables.CATALOG_NAME,  // Name of the catalog
                catalogProperties,  // Catalog properties (e.g., S3 config)
                hadoopConf,  // Hadoop configuration
                "org.apache.iceberg.rest.RESTCatalog"  // Catalog implementation class (REST)
        );


        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of(
                Variables.ICEBERG_DB_NAME, Variables.ICEBERG_DB_TABLE_NAME
                )
        );

        DataStream<Row> rowDataStream = transactionDataStream.map(
                transaction -> Row.of(
                        transaction.getTransactionId(),
                        transaction.getSendingClientAccountNumber(),
                        transaction.getReceivingClientAccountNumber(),
                        transaction.getAmount())
        );


        FlinkSink.forRow(rowDataStream, tableSchema)
                .tableLoader(tableLoader)
                .overwrite(false)
                .append();

        try{
            env.execute(ProcessTransactions.class.getName());
        }
        catch (Exception e){
            e.printStackTrace();
        }


    }
}
