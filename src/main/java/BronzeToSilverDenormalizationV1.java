import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

public class BronzeToSilverDenormalizationV1 {

    public static void main(String[] args) {
        SparkConf config = getSparkConfig();
        SparkSession spark = SparkSession
                .builder()
                .appName("BronzeToSilverDenormalization")
                .master("local")
                .config(config)
                .getOrCreate();

        // CLEAN UP
        //spark.sql("DROP TABLE local.db.OrdersBronze").show();
        //spark.sql("DROP TABLE local.db.OrdersSilver").show();

        // CREATE TABLE OrdersBronze
        String createOrdersBronzeSQL = "CREATE TABLE IF NOT EXISTS local.db.OrdersBronze (" +
                "OrderId string, " +
                "DataSource string, " +
                "DataSourceObject string, " +
                "OrderAmount string, " +
                "InsertDate string) USING iceberg";
        spark.sql(createOrdersBronzeSQL).show();

        // INSERT DATA INTO OrdersBronze - THIS WILL CREATE DUPLICATE RECORDS, BUT STREAMING CAN HANDLE IT
        String insertOrdersBronzeSQL = "INSERT INTO local.db.OrdersBronze VALUES " +
                "('1', 'Orders', 'Orders.csv', '100', '8/1/2021'), " +
                "('2', 'Orders', 'Orders.csv', '20', '8/2/2021'), " +
                "('3', 'Orders', 'Orders.csv', '30', '8/3/2021')";
        spark.sql(insertOrdersBronzeSQL).show();

        // SIMULATE INCOMING BRONZE RECORDS
        String moreInsertOrdersBronzeSQL = "INSERT INTO local.db.OrdersBronze " +
                "SELECT MAX(OrderId) + 1, 'Orders', 'Orders.csv', MAX(OrderAmount) + 10, '8/3/2021' FROM local.db.OrdersBronze";
        spark.sql(moreInsertOrdersBronzeSQL).show();

        // CREATE TABLE OrdersSilver
        String createOrdersSilverSQL = "CREATE TABLE IF NOT EXISTS local.db.OrdersSilver (" +
                "OrderId string, " +
                "DataSource string, " +
                "DataSourceObject string, " +
                "OrderAmount string, " +
                "ShipmentStatus string) USING iceberg";
        spark.sql(createOrdersSilverSQL).show();

        // MERGE INTO - DIRECTLY FROM OrdersBronze TO OrdersSilver
        /*
        String mergeSQL = "MERGE INTO local.db.OrdersSilver target " +
                "USING (SELECT OrderId, DataSource, DataSourceObject, OrderAmount FROM local.db.OrdersBronze) source " +
                "ON source.OrderId = target.OrderId " +
                "WHEN MATCHED THEN UPDATE SET target.OrderAmount = target.OrderAmount + source.OrderAmount " +
                "WHEN NOT MATCHED THEN " +
                "    INSERT (OrderId, DataSource, DataSourceObject, OrderAmount, ShipmentStatus) " +
                "    VALUES (source.OrderId, source.DataSource, source.DataSourceObject, source.OrderAmount, '')";
        spark.sql(mergeSQL).show();
        spark.sql("SELECT * FROM local.db.OrdersSilver").show();
        spark.sql(mergeSQL).show();
        spark.sql("SELECT * FROM local.db.OrdersSilver").show();
        */

        // READ STREAM
        var bronzeDataSet = spark.readStream().format("iceberg")
                .load("local.db.OrdersBronze");

        String mergeSQL1 = "MERGE INTO local.db.OrdersSilver target " +
                "USING sourceDataSet source " +
                "ON source.OrderId = target.OrderId " +
                "WHEN MATCHED THEN " +
                "    UPDATE SET target.OrderAmount = target.OrderAmount + source.OrderAmount " +
                "WHEN NOT MATCHED THEN " +
                "    INSERT (OrderId, DataSource, DataSourceObject, OrderAmount, ShipmentStatus) " +
                "    VALUES (source.OrderId, source.DataSource, source.DataSourceObject, source.OrderAmount, '')";

        // WRITE STREAM WITH MICRO-BATCH
        try {
            StreamingQuery sq = bronzeDataSet.writeStream().format("iceberg")
                    .outputMode(OutputMode.Append())
                    .option("checkpointLocation", "jobsCheckpointDir001")
                    .foreachBatch((ds, batchId) -> {
                        ds.createOrReplaceTempView("sourceDataSet");
                        ds.sparkSession().sql(mergeSQL1).show();
                    })
                    .trigger(Trigger.Once())
                    .start();
            Thread.sleep(60000);
            sq.stop();
        } catch(TimeoutException | InterruptedException ex) {
            System.out.println(ex);
        }

        spark.sql("SELECT * FROM local.db.OrdersBronze").show();
        spark.sql("SELECT * FROM local.db.OrdersSilver").show();

    }

    static private SparkConf getSparkConfig() {
        SparkConf config = new SparkConf();
        config.set("spark.sql.legacy.createHiveTableByDefault", "false");
        config.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        config.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
        config.set("spark.sql.catalog.spark_catalog.type", "hive");
        config.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog");
        config.set("spark.sql.catalog.local.type", "hadoop");
        config.set("spark.sql.catalog.local.warehouse", "spark-warehouse");
        return config;
    }
}
