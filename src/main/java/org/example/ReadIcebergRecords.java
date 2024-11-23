package org.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

import java.util.HashMap;
import java.util.Map;

public class ReadIcebergRecords {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("uri", "http://localhost:8181");  // REST Catalog URI
        catalogProperties.put("warehouse", "s3://warehouse/");  // S3 warehouse location
        catalogProperties.put("io-impl", S3FileIO.class.getName());  // Use S3 File IO
        catalogProperties.put("s3.endpoint", "http://localhost:9000");  // MinIO endpoint http://minio:9000
        catalogProperties.put("s3.access-key", "admin");  // MinIO access key
        catalogProperties.put("s3.secret-key", "password");  // MinIO secret key
        catalogProperties.put("s3.path-style-access", "true");  // Required for MinIO

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.access.key", "admin");
        hadoopConf.set("fs.s3a.secret.key", "password");
        hadoopConf.set("fs.s3a.endpoint", "http://localhost:9000"); //http://minio:9000
        hadoopConf.set("fs.s3a.path.style.access", "true");

        CatalogLoader catalogLoader = CatalogLoader.custom(
                "bank_transactions_catalog",  // Name of the catalog
                catalogProperties,  // Catalog properties (e.g., S3 config)
                hadoopConf,  // Hadoop configuration
                "org.apache.iceberg.rest.RESTCatalog"  // Catalog implementation class (REST)
        );

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of("financial_transactions", "transactions"));
        DataStream<RowData> batch = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                .build();

        // Print all records to stdout.
        batch.print();

        // Submit and execute this batch read job.
        try{
            env.execute(ReadIcebergRecords.class.getName());
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }
}
