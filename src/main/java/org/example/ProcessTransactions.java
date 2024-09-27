package org.example;

import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
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

        final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
        final String username = "postgres";
        final String password = "postgres";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("transaction")
                .setGroupId("transactions_group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(2000L);
        env.enableCheckpointing(30000);  // e.g., 60000 for every minute
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        DataStream<String> dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka source");

        DataStream<Transaction> transactionDataStream = dataStream.map(value ->
                        new Gson().fromJson(value, Transaction.class))
                .assignTimestampsAndWatermarks(new TransactionWatermarkStrategy());

        TableSchema tableSchema = TableSchema.builder()
                .field("transactionId", DataTypes.STRING())
                .field("sendingClientAccountNumber", DataTypes.STRING())
                .field("receivingClientAccountNumber", DataTypes.STRING())
                .field("amount", DataTypes.DOUBLE())
                .build();

        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("uri", "http://localhost:8181");  // REST Catalog URI
        catalogProperties.put("warehouse", "s3://warehouse/");  // S3 warehouse location
        catalogProperties.put("io-impl", S3FileIO.class.getName());  // Use S3 File IO
        catalogProperties.put("s3.endpoint", "http://minio:9000");  // MinIO endpoint
        catalogProperties.put("s3.access-key", "admin");  // MinIO access key
        catalogProperties.put("s3.secret-key", "password");  // MinIO secret key
        catalogProperties.put("s3.path-style-access", "true");  // Required for MinIO

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.access.key", "admin");
        hadoopConf.set("fs.s3a.secret.key", "password");
        hadoopConf.set("fs.s3a.endpoint", "http://minio:9000");
        hadoopConf.set("fs.s3a.path.style.access", "true");

        CatalogLoader catalogLoader = CatalogLoader.custom(
                "my_catalog",  // Name of the catalog
                catalogProperties,  // Catalog properties (e.g., S3 config)
                hadoopConf,  // Hadoop configuration
                "org.apache.iceberg.rest.RESTCatalog"  // Catalog implementation class (REST)
        );

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of("my_db", "transactions"));

        DataStream<Row> rowDataStream = transactionDataStream.map(
                transaction -> Row.of(transaction.getTransactionId(),
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
