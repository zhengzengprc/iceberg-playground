import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.simple.SimpleLogger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.apache.iceberg.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.validation.constraints.AssertTrue;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;


/*
Create initial source Bronze table.
Create initial target Silver tables (empty).
Test different denormalization scenarios.
 */
@RunWith(JUnit4.class)
public class IcebergNormalizationTest {
    private static SparkSession spark;
    private static JavaSparkContext sparkContext;
    private static SparkConf sparkConf;
    private static Dataset<Row> bronzeSparkDataset;
    private static Dataset<Row> silverSparkDataset1;
    private static Dataset<Row> silverSparkDataset2;
    private static Table bronzeTable;
    private static Table silverTable1;
    private static Table silverTable2;
    private static Catalog bronzeCatalog;
    private static TableIdentifier bronzeTableId;
    private static Catalog silverCatalog1;
    private static TableIdentifier silverTableId1;
    private static Catalog silverCatalog2;
    private static TableIdentifier silverTableId2;

    private static final String WAREHOUSE = "warehouse";
    private static final String CATALOG = "local";
    private static final String BRONZE_NAMESPACE = "bronze_namespace";
    private static final String BRONZE_TABLE_NAME = "bronze_table";
    private static final String BRONZE_SQL_TABLE = CATALOG + "." + BRONZE_NAMESPACE + "." + BRONZE_TABLE_NAME;
    private static final String BRONZE_TABLE_PATH = WAREHOUSE + "." + BRONZE_SQL_TABLE;
    private static final String SILVER_NAMESPACE = "silver_namespace";
    private static final String SILVER_TABLE_NAME1 = "silver_table1";
    private static final String SILVER_TABLE_NAME2 = "silver_table2";
    private static final String SILVER_SQL_TABLE1 = CATALOG + "." + SILVER_NAMESPACE + "." + SILVER_TABLE_NAME1;
    private static final String SILVER_SQL_TABLE2 = CATALOG + "." + SILVER_NAMESPACE + "." + SILVER_TABLE_NAME2;
    private static final String SILVER_TABLE_PATH1 = WAREHOUSE + "." + SILVER_SQL_TABLE1;
    private static final String SILVER_TABLE_PATH2 = WAREHOUSE + "." + SILVER_SQL_TABLE2;

    private static Dataset<Row> bronzeSourceStreamDf;
    private static Dataset<Row> silverSinkStreamDf1;

    private static StreamingQuery query;

    /*
    Create source Bronze table.
    Create sink Silver tables.
    Create streaming job.
     */
    @BeforeClass
    public static void setup() {
        setSparkConf();
        setSparkSession();
        createBronzeTable();
        createSilverTables();

        bronzeSourceStreamDf = spark.readStream()
                .format("iceberg")
                .table(BRONZE_SQL_TABLE);

        assertTrue(bronzeSourceStreamDf.isStreaming());

        silverSinkStreamDf1 = bronzeSourceStreamDf.select("id", "firstName", "lastName");


//        var writer = new ForeachWriter<Row>() {
//            @Override
//            public boolean open(long partitionId, long epochId) {
//                return true;
//            }
//
//            @Override
//            public void process(Row value) {
//
//            }
//
//            @Override
//            public void close(Throwable errorOrNull) {
//
//            }
//        }

        try {
            query = silverSinkStreamDf1.writeStream()
//                    .foreach()
                    .format("iceberg")
                    .outputMode("append")
//                    .option("path", SILVER_SQL_TABLE1)
                    .trigger(Trigger.ProcessingTime(1, TimeUnit.MILLISECONDS))
                    .option("checkpointLocation", SILVER_TABLE_NAME1 + "_checkpoint")
                    .toTable(SILVER_SQL_TABLE1);
        } catch (TimeoutException e) {
            System.out.println("PRINTING OUT ERROR STACK TRACE-----------------------------------------");
            e.printStackTrace();
            System.out.println("PRINTING OUT ERROR STACK TRACE-----------------------------------------");
            tearDown();
        }
    }

    /*
    For-each row processing function
     */
    public void processRow(Dataset<String> dataset, Long batchId) {

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
        bronzeCatalog.dropTable(bronzeTableId);
        silverCatalog1.dropTable(silverTableId1);
        silverCatalog2.dropTable(silverTableId2);
        spark.sql("DROP TABLE IF EXISTS " + BRONZE_SQL_TABLE);
        spark.sql("DROP TABLE IF EXISTS " + SILVER_SQL_TABLE1);
        spark.sql("DROP TABLE IF EXISTS " + SILVER_SQL_TABLE2);
    }

    private static void setSparkSession() {
        spark = SparkSession
                .builder()
                .appName("Denormalization Example")
                .master(CATALOG)
                .config(sparkConf)
                .getOrCreate();
        sparkContext = new JavaSparkContext(spark.sparkContext());

    }

    private static void setSparkConf() {
        sparkConf = new SparkConf();
        // spark master URL for distributed cluster: run locally with 1 thread
//        sparkConf.set("spark.master", CATALOG);

        // spark catalog: for non-iceberg tables
//        sparkConf.set("spark.sql.extensions", "org.apache.initialceberg.spark.extensions.IcebergSparkSessionExtensions");
//        sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
//        sparkConf.set("spark.sql.catalog.spark_catalog.type", "hive");

        // local catalog: directory-based in HDFS, for iceberg tables
        sparkConf.set("spark.sql.catalog." + CATALOG, "org.apache.iceberg.spark.SparkCatalog");
        sparkConf.set("spark.sql.catalog." + CATALOG + ".type", "hadoop");
        sparkConf.set("spark.sql.catalog." + CATALOG + ".warehouse", WAREHOUSE);
    }

    private static void createBronzeTable() {
        Schema bronzeSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "firstName", Types.StringType.get()),
                Types.NestedField.optional(3, "lastName", Types.StringType.get()),
                Types.NestedField.optional(4, "streetNo1", Types.IntegerType.get()),
                Types.NestedField.optional(5, "cityName1", Types.StringType.get()),
                Types.NestedField.optional(6, "zipcode1", Types.IntegerType.get()),
                Types.NestedField.optional(7, "county1", Types.StringType.get()),
                Types.NestedField.optional(8, "streetNo2", Types.IntegerType.get()),
                Types.NestedField.optional(9, "cityName2", Types.StringType.get()),
                Types.NestedField.optional(10, "zipcode2", Types.IntegerType.get()),
                Types.NestedField.optional(11, "county2", Types.StringType.get()),
                Types.NestedField.required(12, "arrivalTime", Types.TimestampType.withZone())
        );

        PartitionSpec bronzeSpec = PartitionSpec.builderFor(bronzeSchema)
                .bucket("id",10)
                .build();

        // Catalog method of creating Iceberg table
        bronzeCatalog = new HadoopCatalog(new Configuration(), WAREHOUSE);
        bronzeTableId = TableIdentifier.of(BRONZE_NAMESPACE, BRONZE_TABLE_NAME);
        bronzeTable = bronzeCatalog.createTable(bronzeTableId, bronzeSchema, bronzeSpec);

        bronzeSparkDataset = spark.sql("CREATE TABLE IF NOT EXISTS " + BRONZE_SQL_TABLE +
                "(id bigint, firstName string, lastName string," +
                "streetNo1 int, cityName1 string, zipcode1 int, county1 string," +
                "streetNo2 int, cityName2 string, zipcode2 int, county2 string, arrivalTime timestamp) " +
                "USING iceberg");

//         Table interface method of creating Iceberg table
//        bronzeTable = new HadoopTables().create(bronzeSchema, BRONZE_NAMESPACE + "." + BRONZE_TABLE_NAME);
    }

    private static void createSilverTables() {
        // Indiv. table
        Schema silverSchema1 = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "firstName", Types.StringType.get()),
                Types.NestedField.optional(3, "lastName", Types.StringType.get())
        );

        // Contact Point Address table
        Schema silverSchema2 = new Schema(
                Types.NestedField.required(1, "AddressId", Types.IntegerType.get()),
                Types.NestedField.required(2, "PartyId", Types.IntegerType.get()),
                Types.NestedField.optional(3, "streetNo", Types.IntegerType.get()),
                Types.NestedField.optional(4, "cityName", Types.StringType.get()),
                Types.NestedField.optional(5, "zipcode", Types.IntegerType.get()),
                Types.NestedField.optional(6, "county", Types.StringType.get())
        );

        PartitionSpec silverSpec1 = PartitionSpec.builderFor(silverSchema1)
                .identity("id")
                .bucket("id",10)
                .build();

        PartitionSpec silverSpec2 = PartitionSpec.builderFor(silverSchema2)
                .bucket("PartyId",10)
                .build();

        // Catalog method of creating Iceberg table
        silverCatalog1 = new HadoopCatalog(new Configuration(), WAREHOUSE);
        silverTableId1 = TableIdentifier.of(SILVER_NAMESPACE, SILVER_TABLE_NAME1);
        silverTable1 = silverCatalog1.createTable(silverTableId1, silverSchema1, silverSpec1);
        silverCatalog2 = new HadoopCatalog(new Configuration(), WAREHOUSE);
        silverTableId2 = TableIdentifier.of(SILVER_NAMESPACE, SILVER_TABLE_NAME2);
        silverTable2 = silverCatalog2.createTable(silverTableId2, silverSchema2, silverSpec2);

        silverSparkDataset1 = spark.sql("CREATE TABLE IF NOT EXISTS " + SILVER_SQL_TABLE1 +
                "(id bigint, firstName string, lastName string) " +
                "USING iceberg");

        silverSparkDataset2 = spark.sql("CREATE TABLE IF NOT EXISTS " + SILVER_SQL_TABLE2 +
                "(AddressId bigint, PartyId bigint, " +
                "streetNo int, cityName string, zipcode int, county string) " +
                "USING iceberg");

    }

    /*
    Scenario 1: happy path
     */

    @Test
    public void addEntireRecordTest() {
        bronzeSparkDataset = spark.sql("INSERT INTO " + BRONZE_SQL_TABLE + " VALUES " +
                "(1, \'abc\', \'bcd\', 123, \'redmond\', 98022, \'usa\', 343, \'bellevue\', 98077, \'usa\', current_timestamp())," +
                "(2, \'some\', \'one\', 444, \'seattle\', 98008, \'usa\', NULL, NULL, NULL, NULL, current_timestamp())");

        spark.read().table(SILVER_SQL_TABLE1).show();

        spark.sql("INSERT INTO " + BRONZE_SQL_TABLE + " VALUES " +
                "(3, \'no\', \'one\', 456, \'boston\', 90578, \'usa\', 888, \'san francisco\', 99999, \'usa\', current_timestamp())");

        spark.read().table(SILVER_SQL_TABLE1).show();
    }

}


