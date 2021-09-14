import org.apache.commons.io.FileUtils;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import static org.apache.spark.sql.functions.*;
import static org.junit.Assert.assertTrue;
import org.apache.iceberg.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;
import scala.collection.Seq;

/*
Create initial source Bronze table.
Create initial target Silver tables (empty).
Test different denormalization scenarios.
 */
@RunWith(JUnit4.class)
public class IcebergNormalizationProfileTest {
    private static SparkSession spark;
    private static SparkConf sparkConf;
    private static Dataset<Row> bronzeSparkDataset;
    private static Dataset<Row> silverSparkDataset1;
    private static Dataset<Row> silverSparkDataset2;

    private static Table bronzeTable;
    private static Table silverTable1;
    private static Table silverTable2;

    private static final String WAREHOUSE = "warehouse";
    private static final String CATALOG = "local";
    private static final String BRONZE_NAMESPACE = "bronze_namespace";
    private static final String BRONZE_TABLE_NAME = "bronze_table";
    private static final String BRONZE_SQL_TABLE = CATALOG + "." + BRONZE_NAMESPACE + "." + BRONZE_TABLE_NAME;
    private static final String BRONZE_TABLE_PATH = "warehouse/bronze_namespace/bronze_table";
    private static final String SILVER_NAMESPACE = "silver_namespace";
    private static final String SILVER_TABLE_NAME1 = "silver_table1";
    private static final String SILVER_TABLE_NAME2 = "silver_table2";
    private static final String SILVER_SQL_TABLE1 = CATALOG + "." + SILVER_NAMESPACE + "." + SILVER_TABLE_NAME1;
    private static final String SILVER_SQL_TABLE2 = CATALOG + "." + SILVER_NAMESPACE + "." + SILVER_TABLE_NAME2;
    private static final String SILVER_TABLE_PATH1 = "warehouse/silver_namespace/silver_table1";
    private static final String SILVER_TABLE_PATH2 = "warehouse/silver_namespace/silver_table2";

    private static Dataset<Row> bronzeSourceStreamDf;
    private static Dataset<Row> silverSinkStreamDf;

    private static StreamingQuery query;
    private static VoidFunction2<Dataset<Row>, Long> microBatchHandler;

    /*
    Create source Bronze table.
    Create sink Silver tables.
    Create streaming job.
     */
    @BeforeClass
    public static void setup() throws NoSuchTableException {
        sparkConf = getSparkConf();
        spark = getSparkSession();
        createBronzeTables();
        createSilverTables();
        setupMicroBatchHandler();

        /*
        Profile Bronze Table
         */

        spark.sql("INSERT INTO " + BRONZE_SQL_TABLE + " VALUES " +
                "(1, \'Some\', 2222, null, 3333, 1, current_timestamp(), \'upsert\')");

        bronzeSourceStreamDf = spark.readStream()
                .format("iceberg")
                .load(BRONZE_SQL_TABLE);

        /*
        Stream
         */

        silverSinkStreamDf = bronzeSourceStreamDf.select("*");
        assertTrue(silverSinkStreamDf.isStreaming());

        try {
            query = silverSinkStreamDf.writeStream()
                    .format("iceberg")
                    .outputMode("append")
                    .option("checkpointLocation", "checkpoint")
                    .foreachBatch(microBatchHandler)
                    .start();
            assertTrue(query.isActive());
            query.processAllAvailable();
        } catch (TimeoutException e) {
            e.printStackTrace();
            tearDown();
        }

        System.out.println(BRONZE_SQL_TABLE + " BEGINNING");
        bronzeSparkDataset =  spark.read().format("iceberg").table(BRONZE_SQL_TABLE);
        bronzeSparkDataset.show();
        // local.bronze_namespace.bronze_table
        //+---+----+----+----+----+-------------+--------------------+-----------+
        //| id|name| Ph1| Ph2| Ph3|recordVersion|        _arrivalTime|_recordType|
        //+---+----+----+----+----+-------------+--------------------+-----------+
        //|  1|Some|2222|null|3333|            1|2021-09-12 23:46:...|     upsert|
        //+---+----+----+----+----+-------------+--------------------+-----------+
        System.out.println(SILVER_SQL_TABLE1 + " BEGINNING");
        silverSparkDataset1 =  spark.read().format("iceberg").table(SILVER_SQL_TABLE1);
        silverSparkDataset1.show();
        // local.bronze_namespace.silver_table1
        //+---+----+----+----+----+-------------+
        //| id|name| Ph1| Ph2| Ph3|recordVersion|
        //+---+----+----+----+----+-------------+
        //|  1|Some|2222|null|3333|            1|
        //+---+----+----+----+----+-------------+
        System.out.println(SILVER_SQL_TABLE2 + " BEGINNING");
        silverSparkDataset2 =  spark.read().format("iceberg").table(SILVER_SQL_TABLE2);
        silverSparkDataset2.show();
        // local.bronze_namespace.silver_table2
        //+--------------+-------+----+-------------+
        //|ContactPointId|PartyId|  Ph|recordVersion|
        //+--------------+-------+----+-------------+
        //|           1_3|      1|3333|            1|
        //|           1_1|      1|2222|            1|
        //+--------------+-------+----+-------------+
    }

    /*
    Delete all tables.
     */
    @AfterClass
    public static void tearDown() {
        try {
            query.stop();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        spark.sql("DROP TABLE IF EXISTS " + BRONZE_SQL_TABLE);
        spark.sql("DROP TABLE IF EXISTS " + SILVER_SQL_TABLE1);
        spark.sql("DROP TABLE IF EXISTS " + SILVER_SQL_TABLE2);
        // delete checkpts
        try {
            FileUtils.deleteDirectory(FileUtils.getFile(WAREHOUSE));
            FileUtils.deleteDirectory(FileUtils.getFile("checkpoint")); // TODO: add more folders here as relevant
        } catch (IOException e) {
            e.printStackTrace();
        }
        spark.stop();
    }

    public static void setupMicroBatchHandler() {
        microBatchHandler = new VoidFunction2<Dataset<Row>, Long>() {
            @Override
            public void call(Dataset<Row> ds, Long batchId) throws Exception {
                // TODO data cleanup (omitted for now)

                System.out.println("PROCESSING MICROBATCH " + batchId +" ****************************************");
                SparkSession dsSpark = ds.sparkSession();

                // construct silver1Updates and silver2Updates temp views
                ds.select("id", "name", "Ph1", "Ph2", "Ph3", "recordVersion", "_recordType").createOrReplaceTempView("silver1Updates");
                Dataset<Row> silver21UpdatesDs = ds.selectExpr("id AS PartyId", "Ph1 AS Ph", "recordVersion", "_recordType")
                        .withColumn("ContactPointId", concat(col("PartyId"), lit("_"), lit("1")));
                Dataset<Row> silver22UpdatesDs = ds.selectExpr("id AS PartyId", "Ph2 AS Ph", "recordVersion", "_recordType")
                        .withColumn("ContactPointId", concat(col("PartyId"), lit("_"), lit("2")));
                Dataset<Row> silver23UpdatesDs = ds.selectExpr("id AS PartyId", "Ph3 AS Ph", "recordVersion", "_recordType")
                        .withColumn("ContactPointId", concat(col("PartyId"), lit("_"), lit("3")));
                silver21UpdatesDs.unionAll(silver22UpdatesDs).unionAll(silver23UpdatesDs).createOrReplaceTempView("silver2Updates");

                String idMaxRecordVersionSql = "SELECT id, MAX(recordVersion) AS maxRecordVersion FROM silver1Updates GROUP BY id";
                dsSpark.sql(idMaxRecordVersionSql).createOrReplaceTempView("idMaxRecordVersions");

                // silver 1 merge into semantics
                String rowsForMaxRecordVersionSql_silver1 =
                        "(SELECT a.id, a.name, a.Ph1, a.Ph2, a.Ph3, a.recordVersion, a._recordType " +
                            "FROM silver1Updates AS a " + // get only the rows corresponding to latest recordVersion
                            "INNER JOIN idMaxRecordVersions AS b " +
                            "ON a.id = b.id AND a.recordVersion = b.maxRecordVersion) ";
                String silver1AllNullCondition = "(source.name IS NULL AND source.Ph1 IS NULL AND source.Ph2 IS NULL AND source.Ph3 IS NULL) ";
                String mergeSql_silver1 =
                        "MERGE INTO " + SILVER_SQL_TABLE1 + " AS target " +
                        "USING " + rowsForMaxRecordVersionSql_silver1 +
                        "AS source " +
                        "ON source.id = target.id " +
                        "WHEN MATCHED AND source.recordVersion > target.recordVersion AND " +
                            "(source._recordType = \'delete\' OR "+ silver1AllNullCondition +") THEN " +
                            "DELETE " +
                        "WHEN MATCHED AND source.recordVersion > target.recordVersion AND source._recordType = \'upsert\' THEN " +
                            "UPDATE SET target.id = source.id, target.name = source.name, target.Ph1 = source.Ph1, target.Ph2 = source.Ph2, target.Ph3 = source.Ph3, target.recordVersion = source.recordVersion " +
                        "WHEN NOT MATCHED AND NOT " + silver1AllNullCondition + "THEN " +
                            "INSERT (id, name, Ph1, Ph2, Ph3, recordVersion) VALUES (source.id, source.name, source.Ph1, source.Ph2, source.Ph3, source.recordVersion)";
                dsSpark.sql(mergeSql_silver1);

                // Note: UPDATE SET is not yet supported. To get MERGE INTO to work: https://www.mail-archive.com/dev@iceberg.apache.org/msg01895.html

                // silver 2 merge into semantics
                String rowsForMaxRecordVersionSql_silver2 =
                        "(SELECT a.PartyId, a.ContactPointId, a.Ph, a.recordVersion, a._recordType " +
                                "FROM silver2Updates AS a " + // get only the rows corresponding to latest recordVersion
                                "INNER JOIN idMaxRecordVersions AS b " +
                                "ON a.PartyId = b.id AND a.recordVersion = b.maxRecordVersion) ";
                String silver2AllNullCondition = "source.Ph IS NULL ";
                String mergeSql_silver2 =
                        "MERGE INTO " + SILVER_SQL_TABLE2 + " AS target " +
                        "USING " + rowsForMaxRecordVersionSql_silver2 +
                        "AS source " +
                        "ON source.PartyId = target.PartyId AND source.ContactPointId = target.ContactPointId " +
                        "WHEN MATCHED AND source.recordVersion > target.recordVersion AND " +
                            "(source._recordType = \'delete\' OR " + silver2AllNullCondition + ") THEN " +
                            "DELETE " +
                        "WHEN MATCHED AND source.recordVersion > target.recordVersion AND source._recordType = \'upsert\' THEN " +
                            "UPDATE SET target.PartyId = source.PartyId, target.ContactPointId = source.ContactPointId, target.Ph = source.Ph, target.recordVersion = source.recordVersion " +
                        "WHEN NOT MATCHED AND NOT " + silver2AllNullCondition + " THEN " +
                            "INSERT (PartyId, ContactPointId, Ph, recordVersion) VALUES (source.PartyId, source.ContactPointId, source.Ph, source.recordVersion)";

                dsSpark.sql(mergeSql_silver2);
                spark.sql("REFRESH TABLE " + SILVER_SQL_TABLE1);
                spark.sql("REFRESH TABLE " + SILVER_SQL_TABLE2);
            }
        };
    }

    private static SparkSession getSparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .appName("Denormalization Example")
                .master(CATALOG)
                .config(sparkConf)
                .getOrCreate();
        return spark;
    }

    private static SparkConf getSparkConf() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

        // local catalog: directory-based in HDFS, for iceberg tables
        sparkConf.set("spark.sql.catalog." + CATALOG, "org.apache.iceberg.spark.SparkCatalog");
        sparkConf.set("spark.sql.catalog." + CATALOG + ".type", "hadoop");
        sparkConf.set("spark.sql.catalog." + CATALOG + ".warehouse", WAREHOUSE);
        return sparkConf;
    }

    private static void createBronzeTables() {
        Schema bronzeSchema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "Ph1", Types.IntegerType.get()),
                Types.NestedField.optional(4, "Ph2", Types.IntegerType.get()),
                Types.NestedField.optional(5, "Ph3", Types.IntegerType.get()),
                Types.NestedField.optional(6, "recordVersion", Types.IntegerType.get()),
                Types.NestedField.optional(7, "_arrivalTime", Types.TimestampType.withZone()),
                Types.NestedField.optional(8, "_recordType", Types.StringType.get())
        );

        PartitionSpec bronzeSpec = PartitionSpec.builderFor(bronzeSchema)
                .bucket("id", 10)
                .build();

        // Catalog method of creating Iceberg table
        bronzeTable = createOrReplaceHadoopTable(bronzeSchema, bronzeSpec, new HashMap<>(), BRONZE_TABLE_PATH);

        spark.sql("CREATE TABLE IF NOT EXISTS " + BRONZE_SQL_TABLE +
                "(id int, name string, Ph1 int, Ph2 int, Ph3 int, recordVersion int, _arrivalTime timestamp, _recordType int) " +
                "USING iceberg");
    }

    private static void createSilverTables() {
        // full record overwrite table
        Schema silverSchema1 = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "Ph1", Types.IntegerType.get()),
                Types.NestedField.optional(4, "Ph2", Types.IntegerType.get()),
                Types.NestedField.optional(5, "Ph3", Types.IntegerType.get()),
                Types.NestedField.optional(6, "recordVersion", Types.IntegerType.get())
        );

        // full refresh table
        Schema silverSchema2 = new Schema(
                Types.NestedField.optional(1, "ContactPointId", Types.StringType.get()),
                Types.NestedField.optional(2, "PartyId", Types.IntegerType.get()),
                Types.NestedField.optional(3, "Ph", Types.IntegerType.get()),
                Types.NestedField.optional(4, "recordVersion", Types.IntegerType.get())
        );

        PartitionSpec silverSpec1 = PartitionSpec.builderFor(silverSchema1)
                .identity("id")
                .bucket("id",10)
                .build();

        PartitionSpec silverSpec2 = PartitionSpec.builderFor(silverSchema2)
                .bucket("ContactPointId",10)
                .build();

        // Catalog method of creating Iceberg table
        silverTable1 = createOrReplaceHadoopTable(silverSchema1, silverSpec1, new HashMap<>(), SILVER_TABLE_PATH1);
        silverTable2 = createOrReplaceHadoopTable(silverSchema2, silverSpec2, new HashMap<>(), SILVER_TABLE_PATH2);

        spark.sql("CREATE TABLE IF NOT EXISTS " + SILVER_SQL_TABLE1 +
                "(id int, name string, Ph1 int, Ph2 int, Ph3 int, recordVersion int) " +
                "USING iceberg");
//        spark.sql("ALTER TABLE " + SILVER_SQL_TABLE1 + " ADD CONSTRAINT pk (PRIMARY KEY (id))");

        spark.sql("CREATE TABLE IF NOT EXISTS " + SILVER_SQL_TABLE2 +
                "(ContactPointId string, PartyId int, Ph int, recordVersion int) " +
                "USING iceberg");
//        spark.sql("ALTER TABLE " + SILVER_TABLE_NAME2 + " ADD CONSTRAINT pk (PRIMARY KEY (ContactPointId))");
//        spark.sql("ALTER TABLE " + SILVER_TABLE_NAME2 + " ADD CONSTRAINT fk (FOREIGN KEY(PartyId) REFERENCES " + SILVER_SQL_TABLE1 + " (id))");
        // TODO I think pk and fk constraints for spark sql are still in progress: https://issues.apache.org/jira/browse/SPARK-21784
    }

    private static Table createOrReplaceHadoopTable(Schema schema, PartitionSpec spec, Map<String, String> properties, String tableIdentifier) {
        HadoopTables tables = new HadoopTables(spark.sparkContext().hadoopConfiguration());
        tables.dropTable(tableIdentifier);
        return tables.create(schema, spec, properties, tableIdentifier);
    }

    public static Seq<Column> convertListToSeq(List<String> inputList) {
        List<Column> inputListCols = inputList.stream().map(colName -> col(colName)).collect(Collectors.toList());
        return JavaConverters.asScalaIteratorConverter(inputListCols.iterator()).asScala().toSeq();
    }

    @Test
    public void overwriteInsertTest() throws InterruptedException {
        spark.sql("INSERT INTO " + BRONZE_SQL_TABLE + " VALUES " +
                "(1, \'Some\', 2222, 4444, 3333, 2, current_timestamp(), \'upsert\')");
        query.processAllAvailable();
        System.out.println(BRONZE_SQL_TABLE);
        bronzeSparkDataset =  spark.read().format("iceberg").table(BRONZE_SQL_TABLE);
        bronzeSparkDataset.show();
        //local.bronze_namespace.bronze_table
        //+---+----+----+----+----+-------------+--------------------+-----------+
        //| id|name| Ph1| Ph2| Ph3|recordVersion|        _arrivalTime|_recordType|
        //+---+----+----+----+----+-------------+--------------------+-----------+
        //|  1|Some|2222|4444|3333|            2|2021-09-12 23:46:...|     upsert|
        //|  1|Some|2222|null|3333|            1|2021-09-12 23:46:...|     upsert|
        //+---+----+----+----+----+-------------+--------------------+-----------+
        System.out.println(SILVER_SQL_TABLE1);
        silverSparkDataset1 =  spark.read().format("iceberg").table(SILVER_SQL_TABLE1);
        silverSparkDataset1.show();
        //local.bronze_namespace.silver_table1
        //+---+----+----+----+----+-------------+
        //| id|name| Ph1| Ph2| Ph3|recordVersion|
        //+---+----+----+----+----+-------------+
        //|  1|Some|2222|4444|3333|            2|
        //+---+----+----+----+----+-------------+

        System.out.println(SILVER_SQL_TABLE2);
        silverSparkDataset2 =  spark.read().format("iceberg").table(SILVER_SQL_TABLE2);
        silverSparkDataset2.show();
        // local.bronze_namespace.silver_table2
        //+--------------+-------+----+-------------+
        //|ContactPointId|PartyId|  Ph|recordVersion|
        //+--------------+-------+----+-------------+
        //|           1_2|      1|4444|            2|
        //|           1_3|      1|3333|            2|
        //|           1_1|      1|2222|            2|
        //+--------------+-------+----+-------------+
    }

    @Test
    public void overwriteDeleteTest() throws InterruptedException {
        spark.sql("INSERT INTO " + BRONZE_SQL_TABLE + " VALUES " +
                "(1, \'Some\', null, 5555, 3333, 3, current_timestamp(), \'upsert\')");
        query.processAllAvailable();
        System.out.println(BRONZE_SQL_TABLE);
        bronzeSparkDataset =  spark.read().format("iceberg").table(BRONZE_SQL_TABLE);
        bronzeSparkDataset.show();
        //local.bronze_namespace.bronze_table
        //+---+----+----+----+----+-------------+--------------------+-----------+
        //| id|name| Ph1| Ph2| Ph3|recordVersion|        _arrivalTime|_recordType|
        //+---+----+----+----+----+-------------+--------------------+-----------+
        //|  1|Some|2222|4444|3333|            2|2021-09-12 23:46:...|     upsert|
        //|  1|Some|2222|null|3333|            1|2021-09-12 23:46:...|     upsert|
        //|  1|Some|null|5555|3333|            3|2021-09-12 23:46:...|     upsert|
        //+---+----+----+----+----+-------------+--------------------+-----------+
        System.out.println(SILVER_SQL_TABLE1);
        silverSparkDataset1 =  spark.read().format("iceberg").table(SILVER_SQL_TABLE1);
        silverSparkDataset1.show();
        //local.bronze_namespace.silver_table1
        //+---+----+----+----+----+-------------+
        //| id|name| Ph1| Ph2| Ph3|recordVersion|
        //+---+----+----+----+----+-------------+
        //|  1|Some|null|5555|3333|            3|
        //+---+----+----+----+----+-------------+

        System.out.println(SILVER_SQL_TABLE2);
        silverSparkDataset2 =  spark.read().format("iceberg").table(SILVER_SQL_TABLE2);
        silverSparkDataset2.show();
        // local.bronze_namespace.silver_table2
        //+--------------+-------+----+-------------+
        //|ContactPointId|PartyId|  Ph|recordVersion|
        //+--------------+-------+----+-------------+
        //|           1_2|      1|5555|            3|
        //|           1_3|      1|3333|            3|
        //+--------------+-------+----+-------------+
    }
}


