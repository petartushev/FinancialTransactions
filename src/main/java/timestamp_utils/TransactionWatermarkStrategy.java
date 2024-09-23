package timestamp_utils;

import org.apache.flink.api.common.eventtime.*;
import pojo.Transaction;

public class TransactionWatermarkStrategy implements WatermarkStrategy<Transaction> {

    @Override
    public TimestampAssigner<Transaction> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return (element, record_timestamp) -> element.getTimestamp();
    }

    @Override
    public WatermarkGenerator<Transaction> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<Transaction>() {
            @Override
            public void onEvent(Transaction transaction, long l, WatermarkOutput watermarkOutput) {
                return;
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis()));
            }
        };
    }

}
