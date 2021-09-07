import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.iceberg.expressions.Expressions.ref;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MoveIcebergSpikeTest {
    static private String TEST_FILE_DIR_PATH = "src/main/resources/csvtests";
    private SparkSession spark;

    @Before
    public void setUp() throws Exception {
        SparkConf config = getSparkConfig();
        spark = SparkSession
                .builder()
                .appName("icebergSpike")
                .master("local")
                .config(config)
                .getOrCreate();
    }

    @Test
    public void testIcebergSpikeTesting() throws NoSuchTableException, TimeoutException, StreamingQueryException, InterruptedException {
        final String path = TEST_FILE_DIR_PATH + "/individual_spike.csv";
        final String tableVersion = "2";

        // 1.Create an iceberg table
        final String tableId = "spark-warehouse/db/icebergspiketesting";
        final String targetDBName = "local.db.icebergspiketesting";
        Schema schema = new Schema(
                Types.NestedField.optional(1, "firstName", Types.StringType.get()),
                Types.NestedField.optional(2, "lastName", Types.StringType.get()),
                Types.NestedField.optional(3, "age", Types.StringType.get()),
                Types.NestedField.optional(4, "location", Types.StringType.get())
        );

        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", tableVersion);
        Table table = createOrReplaceHadoopTable(schema, PartitionSpec.unpartitioned(), properties, tableId);

        // 2.Insert (no upserts only inserts) data into the table
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .csv(path);
        df.writeTo(targetDBName).append();
        df.show();

        // 3.Delete rows from the table & make sure that it generates Snapshots of type: Delete
        // df.writeTo(targetDBName).replace();
        // Snapshot of type OVERWRITE?
        //table.newDelete().deleteFromRowFilter(Expressions.notEqual("location", "Seattle")).commit();
        /*spark.sql("DELETE FROM local.db.icebergspiketesting WHERE location = \"Redmond\"");
        df = spark.table(targetDBName);
        df.show();*/

        // 4.Run table maintenance on the iceberg table - to expire snapshots
        // Observer nothing?
        /*Actions.forTable(table).rewriteDataFiles()
                .filter(Expressions.equal("location", "Bellevue"))
                .targetSizeInBytes(500 * 1024 * 1024) // 500 MB
                .execute();*/

        //table.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit();

        // 5.Create a ReadStream out of this Iceberg table and make sure that it works. df.readStream()
        Dataset<Row> df2 = spark.readStream()
                .table(targetDBName);
        assertTrue(df2.isStreaming());
        StreamingQuery query = df2.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start();

        Thread.sleep(2000);
        query.stop();
    }

    @Test
    public void testInsertIntoIcebergAndReadStream() throws TimeoutException, InterruptedException, NoSuchTableException {
        final String path = TEST_FILE_DIR_PATH + "/individual_spike.csv";
        final String tableVersion = "2";

        // 1.Create an iceberg table
        final String tableId = "spark-warehouse/db/icebergspiketesting";
        final String targetDBName = "local.db.icebergspiketesting";
        Schema schema = new Schema(
                Types.NestedField.optional(1, "firstName", Types.StringType.get()),
                Types.NestedField.optional(2, "lastName", Types.StringType.get()),
                Types.NestedField.optional(3, "age", Types.StringType.get()),
                Types.NestedField.optional(4, "location", Types.StringType.get())
        );

        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", tableVersion);
        Table table = createOrReplaceHadoopTable(schema, PartitionSpec.unpartitioned(), properties, tableId);

        // 2.Insert (no upserts only inserts) data into the table
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .csv(path);
        df.writeTo(targetDBName).append();
        df.show();

        // 5.Create a ReadStream out of this Iceberg table and make sure that it works. df.readStream()
        Dataset<Row> df2 = spark.readStream()
                .table(targetDBName);
        assertTrue(df2.isStreaming());
        StreamingQuery query = df2.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start();

        Thread.sleep(2000);
        query.stop();
    }

    @Test
    public void testDeleteRowsInIcebergTable() throws NoSuchTableException {
        final String path = TEST_FILE_DIR_PATH + "/individual_spike.csv";
        final String tableVersion = "2";

        // 1.Create an iceberg table
        final String tableId = "spark-warehouse/db/icebergspiketesting";
        final String targetDBName = "local.db.icebergspiketesting";
        Schema schema = new Schema(
                Types.NestedField.optional(1, "firstName", Types.StringType.get()),
                Types.NestedField.optional(2, "lastName", Types.StringType.get()),
                Types.NestedField.optional(3, "age", Types.StringType.get()),
                Types.NestedField.optional(4, "location", Types.StringType.get())
        );

        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", tableVersion);
        Table table = createOrReplaceHadoopTable(schema, PartitionSpec.unpartitioned(), properties, tableId);

        // ************ WITHOUT THIS PART *************** WILL BE OVERWRITE
        table.updateSpec()
                .addField(ref("location"))
                .commit();

        // 2.Insert (no upserts only inserts) data into the table
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .csv(path);
        df.writeTo(targetDBName).append();
        df.show();

        table.refresh();

        // 3.Delete rows from the table & make sure that it generates Snapshots of type: Delete
        // Snapshot of type OVERWRITE?
        table.newDelete().deleteFromRowFilter(Expressions.equal("location", "Redmond")).commit();
        //spark.sql("DELETE FROM local.db.icebergspiketesting WHERE location = \"Redmond\"");
        df = spark.table(targetDBName);
        table.refresh();
        df.show();
        assertEquals(2, df.count());
    }

    @Test
    public void testMaintenanceIcebergTable() throws NoSuchTableException {
        final String path = TEST_FILE_DIR_PATH + "/individual_spike.csv";
        final String tableVersion = "1";

        // 1.Create an iceberg table
        final String tableId = "spark-warehouse/db/icebergspiketesting";
        final String targetDBName = "local.db.icebergspiketesting";
        Schema schema = new Schema(
                Types.NestedField.optional(1, "firstName", Types.StringType.get()),
                Types.NestedField.optional(2, "lastName", Types.StringType.get()),
                Types.NestedField.optional(3, "age", Types.StringType.get()),
                Types.NestedField.optional(4, "location", Types.StringType.get())
        );

        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", tableVersion);
        Table table = createOrReplaceHadoopTable(schema, PartitionSpec.unpartitioned(), properties, tableId);

        // 2.Insert (no upserts only inserts) data into the table
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .csv(path);
        df.writeTo(targetDBName).append();
        df.show();

        // df.writeTo(targetDBName).replace();
        // df.show();
        // 4.Run table maintenance on the iceberg table - to expire snapshots
        // Observer nothing?
        Actions.forTable(table).rewriteDataFiles()
                .filter(Expressions.equal("location", "Bellevue"))
                .execute();

        table.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit();
    }

    static private SparkConf getSparkConfig() {
        SparkConf config = new SparkConf();
        config.set("spark.sql.legacy.createHiveTableByDefault", "false");
        config.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        config.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog");
        config.set("spark.sql.catalog.local.type", "hadoop");
        config.set("spark.sql.catalog.local.warehouse", "spark-warehouse");
        return config;
    }

    private Table loadHadoopTable(String location) {
        HadoopTables tables = new HadoopTables(spark.sparkContext().hadoopConfiguration());
        return tables.load(location);
    }

    private Table createOrReplaceHadoopTable(Schema schema, PartitionSpec spec, Map<String, String> properties, String tableIdentifier) {
        HadoopTables tables = new HadoopTables(spark.sparkContext().hadoopConfiguration());
        tables.dropTable(tableIdentifier);
        return tables.create(schema, spec, properties, tableIdentifier);
    }
}
