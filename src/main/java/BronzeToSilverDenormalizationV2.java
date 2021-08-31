import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

public class BronzeToSilverDenormalizationV2 {

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
                "InsertDate string, " +
                "Record_Version string) USING iceberg";
        spark.sql(createOrdersBronzeSQL).show();

        // INSERT DATA INTO OrdersBronze - THIS WILL CREATE DUPLICATE RECORDS WITH MULTIPLE RUNS
        // WHILE DEBUGGING, BUT STREAMING CAN HANDLE IT AND IGNORE DUPLICATES.
        String insertOrdersBronzeSQL = "INSERT INTO local.db.OrdersBronze VALUES " +
                "('1', 'Orders', 'Orders.csv', '100', '8/1/2021', 'V1'), " +
                "('2', 'Orders', 'Orders.csv', '20', '8/2/2021', 'V1'), " +
                "('3', 'Orders', 'Orders.csv', '30', '8/3/2021', 'V1'), " +
                "('1', 'Orders', 'Orders.csv', '999', '8/3/2021', 'V2')";
        spark.sql(insertOrdersBronzeSQL).show();

        // SIMULATE INCOMING BRONZE RECORDS
        String moreInsertOrdersBronzeSQL = "INSERT INTO local.db.OrdersBronze " +
                "SELECT MAX(INT(OrderId)) + 1, 'Orders', 'Orders.csv', MAX(OrderAmount) + 10, '8/3/2021', 'V1' FROM local.db.OrdersBronze";
        for(int i = 0; i < 3; i++) {
            spark.sql(moreInsertOrdersBronzeSQL).show();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {

            }
        }

        // CREATE TABLE OrdersSilver
        String createOrdersSilverSQL = "CREATE TABLE IF NOT EXISTS local.db.OrdersSilver (" +
                "OrderId string, " +
                "DataSource string, " +
                "DataSourceObject string, " +
                "OrderAmount string, " +
                "ShipmentStatus string, " +
                "Record_Version string) USING iceberg";
        spark.sql(createOrdersSilverSQL).show();

        // READ STREAM
        var bronzeDataSet = spark.readStream().format("iceberg")
                .load("local.db.OrdersBronze");

        // org.apache.spark.SparkException: The ON search condition of the MERGE statement
        // matched a single row from the target table with multiple rows of the source table.
        // This could result in the target row being operated on more than once with an update
        // or delete operation and is not allowed.

        // MERGE INTO
        // 1). If source Record_Version is greater, update
        // 2). If source Record_Version is smaller or equal, igore
        // 3). If new record, insert
        String mergeSQL1 = "MERGE INTO local.db.OrdersSilver target " +
                "USING (SELECT DISTINCT t1.* FROM sourceDataSet t1 " +
                "       JOIN (SELECT OrderId, MAX(Record_Version) Record_Version " +
                "             FROM sourceDataSet GROUP BY OrderId) t2 " +
                "       ON t1.OrderId = t2.OrderId AND t1.Record_Version = t2.Record_Version" +
                ") source " +
                "ON INT(source.OrderId) = INT(target.OrderId)" +
                "WHEN MATCHED AND source.Record_Version > target.Record_Version THEN " +
                "    UPDATE SET target.OrderAmount = target.OrderAmount + source.OrderAmount " +
                // org.apache.spark.sql.AnalysisException: cannot resolve '`target.OrderId`' given input columns: [source.DataSource, target.DataSource, source.DataSourceObject, target.DataSourceObject, source.InsertDate, source.OrderAmount, target.OrderAmount, source.OrderId, target.OrderId, source.Record_Version, target.Record_Version, target.ShipmentStatus]
                //"WHEN NOT MATCHED AND (source.OrderId != target.OrderId) THEN " +
                // org.apache.spark.sql.AnalysisException: Subqueries are not supported in conditions of MERGE operations.
                //"WHEN NOT MATCHED AND (source.OrderId NOT IN (SELECT OrderId FROM local.db.OrdersSilver)) THEN " +
                "WHEN NOT MATCHED THEN " +
                "    INSERT (OrderId, DataSource, DataSourceObject, OrderAmount, ShipmentStatus, Record_Version) " +
                "    VALUES (source.OrderId, source.DataSource, source.DataSourceObject, source.OrderAmount, '', source.Record_Version)";

        // WRITE STREAM WITH MICRO-BATCH
        try {
            StreamingQuery sq = bronzeDataSet.writeStream().format("iceberg")
                    .outputMode(OutputMode.Append())
                    .option("checkpointLocation", "jobsCheckpointDir008")
                    .option("maxOffsetsPerTrigger", "1") //USELESS
                    .foreachBatch((ds, batchId) -> {
                        ds.createOrReplaceTempView("sourceDataSet");
                        ds.sparkSession().sql(mergeSQL1).show();
                    })
                    //.trigger(Trigger.Once())
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
