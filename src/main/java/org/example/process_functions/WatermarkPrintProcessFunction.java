package org.example.process_functions;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import pojo.Transaction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class WatermarkPrintProcessFunction extends ProcessFunction<Transaction, Transaction> {
    @Override
    public void processElement(Transaction transaction, ProcessFunction<Transaction, Transaction>.Context context, Collector<Transaction> collector) throws Exception {
        collector.collect(transaction);
        System.out.println(transaction);
        System.out.println("Current watermark: " + LocalDateTime.ofInstant(
                Instant.ofEpochMilli(
                        context.timerService().currentWatermark()), ZoneId.systemDefault()
                )
        );
    }
}
