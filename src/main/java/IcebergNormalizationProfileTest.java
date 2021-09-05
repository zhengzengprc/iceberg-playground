import org.apache.commons.io.FileUtils;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import static org.apache.spark.sql.functions.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
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
                "(1, \'Some\', 2222, null, 3333, 1, \'overwrite\')");

        bronzeSourceStreamDf = spark.readStream()
                .format("iceberg")
                .load(BRONZE_SQL_TABLE);


        /*
        Streamz
         */

        silverSinkStreamDf = bronzeSourceStreamDf.select("*");
        assertTrue(silverSinkStreamDf.isStreaming());

        try {
            query = silverSinkStreamDf.writeStream()
                    .format("iceberg")
                    .trigger(Trigger.ProcessingTime("25 seconds")) // default trigger runs micro-batch as soon as it can
                    .outputMode("append")
                    .option("checkpointLocation", "checkpoint")
                    .foreachBatch(microBatchHandler)
                    .start();
            assertTrue(query.isActive());
            Thread.sleep(60000); // make longer
        } catch (TimeoutException | InterruptedException e) {
            e.printStackTrace();
            tearDown();
        }

        System.out.println(BRONZE_SQL_TABLE + " BEGINNING");
        bronzeSparkDataset =  spark.read().format("iceberg").table(BRONZE_SQL_TABLE);
        bronzeSparkDataset.show();
        /*
        local.bronze_namespace.bronze_table
        +---+----+----+----+----+-------------+----------+
        | id|name| Ph1| Ph2| Ph3|recordVersion|recordType|
        +---+----+----+----+----+-------------+----------+
        |  1|Some|2222|null|3333|            1| overwrite|
        +---+----+----+----+----+-------------+----------+
         */
        System.out.println(SILVER_SQL_TABLE1 + " BEGINNING");
        silverSparkDataset1 =  spark.read().format("iceberg").table(SILVER_SQL_TABLE1);
        silverSparkDataset1.show();
        /*
        local.bronze_namespace.silver_table1
        +---+----+----+----+----+-------------+
        | id|name| Ph1| Ph2| Ph3|recordVersion|
        +---+----+----+----+----+-------------+
        |  1|Some|2222|null|3333|            1|
        +---+----+----+----+----+-------------+
         */
        System.out.println(SILVER_SQL_TABLE2 + " BEGINNING");
        silverSparkDataset2 =  spark.read().format("iceberg").table(SILVER_SQL_TABLE2);
        silverSparkDataset2.show();
        /*
        local.bronze_namespace.silver_table2
        +--------------+-------+----+-------------+
        |ContactPointId|PartyId|  Ph|recordVersion|
        +--------------+-------+----+-------------+
        |             1|      1|2222|            1|
        |             1|      3|3333|            1|
        +--------------+-------+----+-------------+
         */
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
                // multiple id's with same record version --> choose the one with latest arrivalTime or random/arbitrary
                // null id --> don't merge into the silver table, filter it out
                // null record version --> insert
                // TODO data cleanup

                System.out.println("PROCESSING MICROBATCH " + batchId +" ****************************************");
                SparkSession dsSpark = ds.sparkSession();
                ds.select("id", "recordVersion").createOrReplaceTempView("updates");
                ds.select("id", "name", "Ph1", "Ph2", "Ph3", "recordVersion", "recordType").createOrReplaceTempView("silver1Updates");
                ds.selectExpr("id AS PartyId", "Ph1 AS Ph", "recordVersion", "recordType")
                        .withColumn("ContactPointId", concat(col("PartyId"), lit("_"), lit("1")))
                        .createOrReplaceTempView("silver21Updates");
                ds.selectExpr("id AS PartyId", "Ph2 AS Ph", "recordVersion", "recordType")
                        .withColumn("ContactPointId", concat(col("PartyId"), lit("_"), lit("2")))
                        .createOrReplaceTempView("silver22Updates");
                ds.selectExpr("id AS PartyId", "Ph3 AS Ph", "recordVersion", "recordType")
                        .withColumn("ContactPointId", concat(col("PartyId"), lit("_"), lit("3")))
                        .createOrReplaceTempView("silver23Updates");
                //dsSpark.sql("select * from silver21Updates").show();
                //+-------+----+-------------+----------+--------------+
                //|PartyId|  Ph|recordVersion|recordType|ContactPointId|
                //+-------+----+-------------+----------+--------------+
                //|      1|2222|            1| overwrite|           1_1|
                //+-------+----+-------------+----------+--------------+
                String idMaxRecordVersionSql = "SELECT id, MAX(recordVersion) AS maxRecordVersion FROM updates GROUP BY id";
                dsSpark.sql(idMaxRecordVersionSql).createOrReplaceTempView("idMaxRecordVersions");


                String rowsForMaxRecordVersionSql_silver1 =
                        "(SELECT a.id, a.name, a.Ph1, a.Ph2, a.Ph3, a.recordVersion, a.recordType " +
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
                            "(source.recordType = \'delete\' OR "+ silver1AllNullCondition +") THEN " +
                            "DELETE " +
                        "WHEN MATCHED AND source.recordVersion > target.recordVersion AND source.recordType = \'overwrite\' THEN " +
                            "UPDATE SET target.id = source.id, target.name = source.name, target.Ph1 = source.Ph1, target.Ph2 = source.Ph2, target.Ph3 = source.Ph3, target.recordVersion = source.recordVersion " +
                        "WHEN NOT MATCHED AND NOT " + silver1AllNullCondition + "THEN " +
                            "INSERT (id, name, Ph1, Ph2, Ph3, recordVersion) VALUES (source.id, source.name, source.Ph1, source.Ph2, source.Ph3, source.recordVersion)";
                dsSpark.sql(mergeSql_silver1);
//                dsSpark.sql("select * from " + SILVER_SQL_TABLE1).show();

//                dsSpark.sql("UPDATE silver21Updates SET Ph_no1 = CONCAT(CAST(id AS string), \"_\", Ph_no1)");
//                dsSpark.sql("UPDATE silver22Updates SET Ph_no2 = CONCAT(CAST(id AS string), \"_\", Ph_no2)");
//                dsSpark.sql("UPDATE silver23Updates SET Ph_no3 = CONCAT(CAST(id AS string), \"_\", Ph_no3)");
                // TODO UPDATE SET is not yet supported. To get MERGE INTO to work: https://www.mail-archive.com/dev@iceberg.apache.org/msg01895.html

                String silver2AllNullCondition = "source.Ph IS NULL ";
                String rowsForMaxRecordVersionSql_silver21 =
                        "(SELECT a.PartyId, a.ContactPointId, a.Ph, a.recordVersion, a.recordType " +
                                "FROM silver21Updates AS a " + // get only the rows corresponding to latest recordVersion
                                "INNER JOIN idMaxRecordVersions AS b " +
                                "ON a.PartyId = b.id AND a.recordVersion = b.maxRecordVersion) ";

                String rowsForMaxRecordVersionSql_silver22 =
                        "(SELECT a.PartyId, a.ContactPointId, a.Ph, a.recordVersion, a.recordType " +
                                "FROM silver22Updates AS a " + // get only the rows corresponding to latest recordVersion
                                "INNER JOIN idMaxRecordVersions AS b " +
                                "ON a.PartyId = b.id AND a.recordVersion = b.maxRecordVersion) ";

                String rowsForMaxRecordVersionSql_silver23 =
                        "(SELECT a.PartyId, a.ContactPointId, a.Ph, a.recordVersion, a.recordType " +
                                "FROM silver23Updates AS a " + // get only the rows corresponding to latest recordVersion
                                "INNER JOIN idMaxRecordVersions AS b " +
                                "ON a.PartyId = b.id AND a.recordVersion = b.maxRecordVersion) ";

                String mergeSql_silver21 =
                        "MERGE INTO " + SILVER_SQL_TABLE2 + " AS target " +
                        "USING " +rowsForMaxRecordVersionSql_silver21 +
                        "AS source " +
                        "ON source.PartyId = target.PartyId AND source.ContactPointId = target.ContactPointId " +
                        "WHEN MATCHED AND source.recordVersion > target.recordVersion AND " +
                            "(source.recordType = \'delete\' OR " + silver2AllNullCondition + ") THEN " +
                            "DELETE " +
                        "WHEN MATCHED AND source.recordVersion > target.recordVersion AND source.recordType = \'overwrite\' THEN " +
                            "UPDATE SET target.PartyId = source.PartyId, target.ContactPointId = source.ContactPointId, target.Ph = source.Ph, target.recordVersion = source.recordVersion " +
                        "WHEN NOT MATCHED AND NOT " + silver2AllNullCondition + " THEN " +
                            "INSERT (PartyId, ContactPointId, Ph, recordVersion) VALUES (source.PartyId, source.ContactPointId, source.Ph, source.recordVersion)";

                String mergeSql_silver22 =
                        "MERGE INTO " + SILVER_SQL_TABLE2 + " AS target " +
                        "USING " +rowsForMaxRecordVersionSql_silver22 +
                        "AS source " +
                        "ON source.PartyId = target.PartyId AND source.ContactPointId = target.ContactPointId " +
                        "WHEN MATCHED AND source.recordVersion > target.recordVersion AND " +
                            "(source.recordType = \'delete\' OR " + silver2AllNullCondition + ") THEN " +
                            "DELETE " +
                        "WHEN MATCHED AND source.recordVersion > target.recordVersion AND source.recordType = \'overwrite\' THEN " +
                            "UPDATE SET target.PartyId = source.PartyId, target.ContactPointId = source.ContactPointId, target.Ph = source.Ph, target.recordVersion = source.recordVersion " +
                        "WHEN NOT MATCHED AND NOT " + silver2AllNullCondition + " THEN " +
                            "INSERT (PartyId, ContactPointId, Ph, recordVersion) VALUES (source.PartyId, source.ContactPointId, source.Ph, source.recordVersion)";

                String mergeSql_silver23 =
                        "MERGE INTO " + SILVER_SQL_TABLE2 + " AS target " +
                        "USING " +rowsForMaxRecordVersionSql_silver23 +
                        "AS source " +
                        "ON source.PartyId = target.PartyId AND source.ContactPointId = target.ContactPointId " +
                        "WHEN MATCHED AND source.recordVersion > target.recordVersion AND " +
                            "(source.recordType = \'delete\' OR " + silver2AllNullCondition + ") THEN " +
                            "DELETE " +
                        "WHEN MATCHED AND source.recordVersion > target.recordVersion AND source.recordType = \'overwrite\' THEN " +
                            "UPDATE SET target.PartyId = source.PartyId, target.ContactPointId = source.ContactPointId, target.Ph = source.Ph, target.recordVersion = source.recordVersion " +
                        "WHEN NOT MATCHED AND NOT " + silver2AllNullCondition + " THEN " +
                            "INSERT (PartyId, ContactPointId, Ph, recordVersion) VALUES (source.PartyId, source.ContactPointId, source.Ph, source.recordVersion)";


                dsSpark.sql(mergeSql_silver21);
                dsSpark.sql(mergeSql_silver22);
                dsSpark.sql(mergeSql_silver23);
                spark.sql("REFRESH TABLE " + SILVER_SQL_TABLE1);
                spark.sql("REFRESH TABLE " + SILVER_SQL_TABLE2);
                spark.sql("select * from " + SILVER_SQL_TABLE1).show();
                //+---+----+----+----+----+-------------+
                //| id|name| Ph1| Ph2| Ph3|recordVersion|
                //+---+----+----+----+----+-------------+
                //|  1|Some|2222|null|3333|            1|
                //+---+----+----+----+----+-------------+
                spark.sql("select * from " + SILVER_SQL_TABLE2).show();
                //+--------------+-------+----+-------------+
                //|ContactPointId|PartyId|  Ph|recordVersion|
                //+--------------+-------+----+-------------+
                //|             1|    1_3|3333|            1|
                //|             1|    1_1|2222|            1|
                //+--------------+-------+----+-------------+
                //+--------------+-------+----+-------------+
                //|ContactPointId|PartyId|  Ph|recordVersion|
                //+--------------+-------+----+-------------+
                //|           1_3|      1|3333|            1|
                //|           1_1|      1|2222|            1|
                //+--------------+-------+----+-------------+
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
        // local catalog: directory-based in HDFS, for iceberg tables
//        sparkConf.set("spark.sql.legacy.createHiveTableByDefault", "false");
        sparkConf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
//        sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
//        sparkConf.set("spark.sql.catalog.spark_catalog.type", "hive");
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
                Types.NestedField.optional(7, "recordType", Types.StringType.get())
        );

        PartitionSpec bronzeSpec = PartitionSpec.builderFor(bronzeSchema)
                .bucket("id", 10)
                .build();

        // Catalog method of creating Iceberg table
        bronzeTable = createOrReplaceHadoopTable(bronzeSchema, bronzeSpec, new HashMap<>(), BRONZE_TABLE_PATH);

        spark.sql("CREATE TABLE IF NOT EXISTS " + BRONZE_SQL_TABLE +
                "(id int, name string, Ph1 int, Ph2 int, Ph3 int, recordVersion int, recordType int) " +
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
                "(id int, name string, Ph1 int, Ph2 int, Ph3 int, recordVersion int, recordType string) " +
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
    public void overwriteInsertTest() {
        spark.sql("INSERT INTO " + BRONZE_SQL_TABLE + " VALUES " +
                "(1, \'Some\', 2222, 4444, 3333, 2, \'overwrite\')");
    }

    @Test
    public void overwriteDeleteTest() {
        spark.sql("INSERT INTO " + BRONZE_SQL_TABLE + " VALUES " +
                "(1, \'Some\', null, 5555, 3333, 3, \'overwrite\')");
    }

//    @Test
//    public void addEntireRecordTest() throws InterruptedException {
//        spark.sql("INSERT INTO " + BRONZE_SQL_TABLE + " VALUES " +
//                "(3, \'no\', \'one\', 456, \'boston\', 90578, \'usa\', 888, \'san francisco\', 99999, \'usa\', current_timestamp(), 1)");
//
//        Thread.sleep(5000);
//
//        System.out.println(BRONZE_SQL_TABLE + " AFTER NEW RECORD INSERT");
//        spark.read().format("iceberg").table(BRONZE_SQL_TABLE).show();
//
//        /*
//        local.bronze_namespace.bronze_table
//        +---+---------+--------+---------+---------+--------+-------+---------+-------------+--------+-------+--------------------+-------------+
//        | id|firstName|lastName|streetNo1|cityName1|zipcode1|country1|streetNo2|    cityName2|zipcode2|country2|         arrivalTime|recordVersion|
//        +---+---------+--------+---------+---------+--------+-------+---------+-------------+--------+-------+--------------------+-------------+
//        |  1|      abc|     bcd|      123|  redmond|   98022|    usa|      343|     bellevue|   98077|    usa|2021-08-31 11:19:...|            1|
//        |  2|     some|     one|      444|  seattle|   98008|    usa|     null|         null|    null|   null|2021-08-31 11:19:...|            1|
//        |  3|       no|     one|      456|   boston|   90578|    usa|      888|san francisco|   99999|    usa|2021-08-31 11:19:...|            1| <--
//        +---+---------+--------+---------+---------+--------+-------+---------+-------------+--------+-------+--------------------+-------------+
//         */
//
//        System.out.println(SILVER_SQL_TABLE1 + " AFTER NEW RECORD INSERT");
//        spark.read().format("iceberg").table(SILVER_SQL_TABLE1).show();
//        /*
//        local.silver_namespace.silver_table1
//        +---+---------+--------+-------------+
//        | id|firstName|lastName|recordVersion|
//        +---+---------+--------+-------------+
//        |  3|       no|     one|            1| <--
//        |  1|      abc|     bcd|            1|
//        |  2|     some|     one|            1|
//        +---+---------+--------+-------------+
//         */
//    }
//
//    /*
//    Scenario 2: update existing records
//    Update an existing bronze record (new recordVersion). Corresponding row(s) in silver table(s) should be updated.
//     */
//
//    @Test
//    public void updateExistingRecordTest() throws InterruptedException {
//        spark.sql("INSERT INTO " + BRONZE_SQL_TABLE + " VALUES " +
//                "(2, \'some\', \'body\', 444, \'seattle\', 98008, \'usa\', null, null, null, null, current_timestamp(), 2)");
//
//        Thread.sleep(5000);
//
//        System.out.println(BRONZE_SQL_TABLE + " AFTER EXISTING RECORD UPDATE");
//        spark.read().format("iceberg").table(BRONZE_SQL_TABLE).show();
//
//        /*
//        local.bronze_namespace.bronze_table
//        +---+---------+--------+---------+---------+--------+-------+---------+---------+--------+-------+--------------------+-------------+
//        | id|firstName|lastName|streetNo1|cityName1|zipcode1|country1|streetNo2|cityName2|zipcode2|country2|         arrivalTime|recordVersion|
//        +---+---------+--------+---------+---------+--------+-------+---------+---------+--------+-------+--------------------+-------------+
//        |  1|      abc|     bcd|      123|  redmond|   98022|    usa|      343| bellevue|   98077|    usa|2021-08-31 13:46:...|            1|
//        |  2|     some|     one|      444|  seattle|   98008|    usa|     null|     null|    null|   null|2021-08-31 13:46:...|            1|
//        |  2|     some|    body|      444|  seattle|   98008|    usa|     null|     null|    null|   null|2021-08-31 13:46:...|            2| <--
//        +---+---------+--------+---------+---------+--------+-------+---------+---------+--------+-------+--------------------+-------------+
//         */
//
//        System.out.println(SILVER_SQL_TABLE1 + " AFTER EXISTING RECORD UPDATE");
//        spark.read().format("iceberg").table(SILVER_SQL_TABLE1).show();
//        /*
//        local.silver_namespace.silver_table1
//        +---+---------+--------+-------------+
//        | id|firstName|lastName|recordVersion|
//        +---+---------+--------+-------------+
//        |  2|     some|    body|            2| <--
//        |  1|      abc|     bcd|            1|
//        +---+---------+--------+-------------+
//         */
//    }
//
//        /*
//    Scenario 3: don't update existing records
//    "Update" an existing bronze record (old recordVersion). Corresponding row(s) in silver table(s) should NOT be updated.
//     */
//
//    @Test
//    public void dontUpdateExistingRecordTest() throws InterruptedException {
//        spark.sql("INSERT INTO " + BRONZE_SQL_TABLE + " VALUES " +
//                "(2, \'some\', \'one\', 444, \'seattle\', 98008, \'usa\', null, null, null, null, current_timestamp(), 1)");
//
//        Thread.sleep(5000);
//
//        System.out.println(BRONZE_SQL_TABLE + " AFTER EXISTING RECORD \"UPDATE\"");
//        spark.read().format("iceberg").table(BRONZE_SQL_TABLE).show();
//
//        /*
//        local.bronze_namespace.bronze_table
//        +---+---------+--------+---------+---------+--------+-------+---------+---------+--------+-------+--------------------+-------------+
//        | id|firstName|lastName|streetNo1|cityName1|zipcode1|country1|streetNo2|cityName2|zipcode2|country2|         arrivalTime|recordVersion|
//        +---+---------+--------+---------+---------+--------+-------+---------+---------+--------+-------+--------------------+-------------+
//        |  2|     some|     one|      444|  seattle|   98008|    usa|     null|     null|    null|   null|2021-08-31 13:48:...|            1|
//        |  1|      abc|     bcd|      123|  redmond|   98022|    usa|      343| bellevue|   98077|    usa|2021-08-31 13:48:...|            1|
//        |  2|     some|     one|      444|  seattle|   98008|    usa|     null|     null|    null|   null|2021-08-31 13:48:...|            1| <--
//        +---+---------+--------+---------+---------+--------+-------+---------+---------+--------+-------+--------------------+-------------+
//         */
//
//        System.out.println(SILVER_SQL_TABLE1 + " AFTER EXISTING RECORD \"UPDATE\"");
//        spark.read().format("iceberg").table(SILVER_SQL_TABLE1).show();
//        /*
//        local.silver_namespace.silver_table1
//        +---+---------+--------+-------------+
//        | id|firstName|lastName|recordVersion|
//        +---+---------+--------+-------------+
//        |  1|      abc|     bcd|            1|
//        |  2|     some|     one|            1| <-- NO CHANGE
//        +---+---------+--------+-------------+
//         */
//    }

}


