package org.example;

import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.process_functions.WatermarkPrintProcessFunction;
import org.example.sinks.ExactlyOncePostgresSink;
import org.example.sinks.PostgresSink;
import pojo.Transaction;
import timestamp_utils.TransactionWatermarkStrategy;


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

//        transactionDataStream.print();

        transactionDataStream.addSink(new ExactlyOncePostgresSink(jdbcUrl, username, password));

        try{
            env.execute(ProcessTransactions.class.getName());
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }
}
