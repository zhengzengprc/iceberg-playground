import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

public class BronzeToSilverDenormalizationV3 {

    public static void main(String[] args) {
        SparkConf config = getSparkConfig();
        SparkSession spark = SparkSession
                .builder()
                .appName("BronzeToSilverDenormalization")
                .master("local")
                .config(config)
                .getOrCreate();

        // CLEAN UP - drop tables, rm physical data directory, rm physical checkpoint directory, rename checkpoint.
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
                //"('1', 'Orders', 'Orders.csv', '999', '8/3/2021', 'V2'), " +
                "('1', 'Orders', 'Orders.csv', '888', '8/1/2021', 'V1'), " +
                "(null, 'Orders', 'Orders.csv', '777', '8/1/2021', 'V1'), " +
                "('2', 'Orders', 'Orders.csv', '666', '8/1/2021', null), " +
                "(null, 'Orders', 'Orders.csv', '555', '8/1/2021', 'V8'), " +
                "('3', 'Orders', 'Orders.csv', '444', '8/1/2021', '   '), " +
                "('', 'Orders', 'Orders.csv', '333', '8/1/2021', 'V1')";
        // NULL Record_Version
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
        // NOTE: Although DISTINCT can prevent exact duplicate rows, matched multiple source rows
        // is still possible when columns besides PK/Record_Version are different. ??? However, this is
        // a bronze table data anomaly - e.g. same PK, same Record_Version, different amount.

        // MERGE INTO
        // 1). MATCHED: If PK matches AND source Record_Version is greater, update
        // 2). MATCHED: If PK matches AND source Record_Version is smaller or equal, ignore
        // 3). UNMATCHED: If PK does not match - new record, insert
        String mergeSQL1 = "MERGE INTO local.db.OrdersSilver target " +
                "USING (SELECT t1.* FROM sourceDataSet t1 " +
                "       JOIN (SELECT OrderId, MAX(Record_Version) Record_Version " +
                "             FROM sourceDataSet GROUP BY OrderId) t2 " +
                "       ON t1.OrderId = t2.OrderId AND t1.Record_Version = t2.Record_Version" +
                ") source " +
                //This merge condition has unnecessary update when Record_Version is equal;
                //Also it may cause duplicates in silver table - when the batch has lower Record_Version (possible when new batch
                //has lower versions only, when higher versions were already processed in previous batches).
                //"ON INT(source.OrderId) = INT(target.OrderId) AND source.Record_Version >= target.Record_Version " +
                "ON INT(source.OrderId) = INT(target.OrderId) " +
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
                    .option("checkpointLocation", "jobsCheckpointDir010")
                    .option("maxOffsetsPerTrigger", "1") //USELESS: Kafka supports this option, but it's per partition, so batch size may still be multiples.
                    .foreachBatch((ds, batchId) -> {
                        // Bronze data cleansing logic
                        // 1. Remove null/empty PK/Record_Version records
                        // 2. Drop duplicates based on PK/Record_Version, keeping the latest record from duplicates
                        ds.show();
                        ds.filter("OrderId is not NULL AND Record_Version is not NULL AND trim(OrderId) != '' AND trim(Record_Version) != ''")
                                .orderBy(ds.col("InsertDate").desc())
                                .dropDuplicates("OrderId", "Record_Version")
                                .withColumn("row",
                                        functions.row_number()
                                                .over(Window.partitionBy("OrderId")
                                                        .orderBy(ds.col("Record_Version")
                                                                .desc())))
                                .where("row == 1").drop("row")
                                //.groupBy("OrderId")
                                //.reduce((ReduceFunction<String>)(x, y) -> x.col("Record_Version") > y.col("Record_Version") ? x : y)
                                .createOrReplaceTempView("sourceDataSet");
                        ds.sparkSession().sql("SELECT * FROM sourceDataSet").show();
                        ds.sparkSession().sql(mergeSQL1).show();
                    })
                    //.trigger(Trigger.Once())
                    .start();
            Thread.sleep(120000);
            sq.stop();
        } catch(TimeoutException | InterruptedException ex) {
            System.out.println(ex);
        }

        /*
BEFORE CLEANSING:
+-------+----------+----------------+-----------+----------+--------------+
|OrderId|DataSource|DataSourceObject|OrderAmount|InsertDate|Record_Version|
+-------+----------+----------------+-----------+----------+--------------+
|      1|    Orders|      Orders.csv|        100|  8/1/2021|            V1|
|      2|    Orders|      Orders.csv|         20|  8/2/2021|            V1|
|      3|    Orders|      Orders.csv|         30|  8/3/2021|            V1|
|      1|    Orders|      Orders.csv|        888|  8/1/2021|            V1|
|   null|    Orders|      Orders.csv|        777|  8/1/2021|            V1|
|      2|    Orders|      Orders.csv|        666|  8/1/2021|          null|
|   null|    Orders|      Orders.csv|        555|  8/1/2021|            V8|
|      3|    Orders|      Orders.csv|        444|  8/1/2021|              |
|       |    Orders|      Orders.csv|        333|  8/1/2021|            V1|
|     34|    Orders|      Orders.csv|     1009.0|  8/3/2021|            V1|
|     35|    Orders|      Orders.csv|     1009.0|  8/3/2021|            V1|
|     36|    Orders|      Orders.csv|     1009.0|  8/3/2021|            V1|
+-------+----------+----------------+-----------+----------+--------------+

AFTER CLEANSING:
+-------+----------+----------------+-----------+----------+--------------+
|OrderId|DataSource|DataSourceObject|OrderAmount|InsertDate|Record_Version|
+-------+----------+----------------+-----------+----------+--------------+
|      3|    Orders|      Orders.csv|         30|  8/3/2021|            V1|
|     34|    Orders|      Orders.csv|     1009.0|  8/3/2021|            V1|
|     35|    Orders|      Orders.csv|     1009.0|  8/3/2021|            V1|
|      1|    Orders|      Orders.csv|        100|  8/1/2021|            V1|
|     36|    Orders|      Orders.csv|     1009.0|  8/3/2021|            V1|
|      2|    Orders|      Orders.csv|         20|  8/2/2021|            V1|
+-------+----------+----------------+-----------+----------+--------------+
         */

        spark.sql("SELECT * FROM local.db.OrdersBronze").show(50,false);
        spark.sql("SELECT * FROM local.db.OrdersSilver").show(50,false);

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
