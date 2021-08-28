import org.apache.commons.io.FileUtils;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import org.apache.iceberg.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import scala.collection.JavaConverters;
import scala.collection.Seq;

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
    private static Catalog catalog;
    private static TableIdentifier bronzeTableId;
    private static TableIdentifier silverTableId1;
    private static TableIdentifier silverTableId2;

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
    private static Dataset<Row> silverSinkStreamDf1;
    private static Dataset<Row> silverSinkStreamDf2_1;

    private static Map<String, String> bronzeToSilver1SchemaMap;
    private static List<Map<String, String>> bronzeToSilver2SchemaMap;

    private static StreamingQuery query;

    private static ForeachWriter<Row> silver1Writer;
    private static String silver1WriteMode = "append"; // append, complete, update

    /*
    Create source Bronze table.
    Create sink Silver tables.
    Create streaming job.
     */
    @BeforeClass
    public static void setup() throws NoSuchTableException {
        setSparkConf();
        setSparkSession();
        createBronzeTable();
        createSilverTables();
        setupSchemaMappings();
        setupWriters();

        spark.sql("INSERT INTO " + BRONZE_SQL_TABLE + " VALUES " +
                "(1, \'abc\', \'bcd\', 123, \'redmond\', 98022, \'usa\', 343, \'bellevue\', 98077, \'usa\', current_timestamp())," +
                "(2, \'some\', \'one\', 444, \'seattle\', 98008, \'usa\', NULL, NULL, NULL, NULL, current_timestamp())");

//        spark.table(BRONZE_SQL_TABLE).show();
        bronzeSourceStreamDf = spark.readStream()
                .format("iceberg")
                .table(BRONZE_SQL_TABLE);

        List<String> bronzeSilver1Cols = new ArrayList<String>(bronzeToSilver1SchemaMap.keySet());
        silverSinkStreamDf1 = bronzeSourceStreamDf.select(convertListToSeq(bronzeSilver1Cols)); // String col, Seq<String> cols

        assertTrue(silverSinkStreamDf1.isStreaming());

        try {
            query = silverSinkStreamDf1.writeStream()
//                    .trigger(Trigger.Continuous(1000))
                    .foreach(silver1Writer) // may need modify to match Xiang's, group by record Id's, max cap on the batch
                    // use merge into semantics
                    // batch == all records with same id
                    // merge set of rows into silver
                    // batch may be too big so see max batch size
                    // silver table will have record version
                    .format("iceberg")
                    .outputMode(silver1WriteMode)
                    .option("path", SILVER_SQL_TABLE1)
                    .option("checkpointLocation", SILVER_TABLE_NAME1 + "_checkpoint")
                    .start();

            Thread.sleep(2000);
            assertTrue(query.isActive());
        } catch (TimeoutException | InterruptedException e) {
            e.printStackTrace();
            tearDown();
        }
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
            FileUtils.deleteDirectory(FileUtils.getFile(SILVER_TABLE_NAME1 + "_checkpoint")); // TODO: add more folders here as relevant
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void setupWriters() {
        silver1Writer = new ForeachWriter<Row>() {
            @Override
            public boolean open(long partitionId, long epochId) {
                return true;
            }

            @Override
            public void process(Row value) { // TODO make more generic (i.e. using schema maps)
                String id = value.getAs("id").toString();
                spark.table(BRONZE_SQL_TABLE).show();
                boolean idNotInSilver1 = spark.sql("SELECT * FROM " + SILVER_TABLE_NAME1 + " WHERE id = " + id).isEmpty();
                if (idNotInSilver1) {
                    silver1WriteMode = "append"; // something along these lines
                } else {
                    silver1WriteMode = "update"; // something along these lines
                }
                System.out.println(silver1WriteMode);
            }

            @Override
            public void close(Throwable errorOrNull) {} // do nothing
        };
    }

    private static void setupSchemaMappings() {
        bronzeToSilver1SchemaMap = new LinkedHashMap<>();
        bronzeToSilver1SchemaMap.put("id", "id");
        bronzeToSilver1SchemaMap.put("firstName", "firstName");
        bronzeToSilver1SchemaMap.put("lastName", "lastName");

        Map<String, String> idMap = new LinkedHashMap<>();
        idMap.put("id", "PartyId");

        Map<String, String> address1Map = new LinkedHashMap<>();
        address1Map.put("streetNo1", "streetNo");
        address1Map.put("cityName1", "cityName");
        address1Map.put("zipcode1", "zipcode");
        address1Map.put("county1", "county");

        Map<String, String> address2Map = new LinkedHashMap<>();
        address2Map.put("streetNo2", "streetNo");
        address2Map.put("cityName2", "cityName");
        address2Map.put("zipcode2", "zipcode");
        address2Map.put("county2", "county");
        bronzeToSilver2SchemaMap = List.of(idMap, address1Map, address2Map); // TODO write id generation logic in quip
    }

    private static void setSparkSession() {
        spark = SparkSession
                .builder()
                .appName("Denormalization Example")
                .master(CATALOG)
                .config(sparkConf)
                .getOrCreate();

    }

    private static void setSparkConf() {
        sparkConf = new SparkConf();
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
        bronzeTable = createOrReplaceHadoopTable(bronzeSchema, bronzeSpec, new HashMap<>(), BRONZE_TABLE_PATH);

        spark.sql("CREATE TABLE IF NOT EXISTS " + BRONZE_SQL_TABLE +
                "(id bigint, firstName string, lastName string," +
                "streetNo1 int, cityName1 string, zipcode1 int, county1 string," +
                "streetNo2 int, cityName2 string, zipcode2 int, county2 string, arrivalTime timestamp) " +
                "USING iceberg");
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
        silverTable1 = createOrReplaceHadoopTable(silverSchema1, silverSpec1, new HashMap<>(), SILVER_TABLE_PATH1);
        silverTable2 = createOrReplaceHadoopTable(silverSchema2, silverSpec2, new HashMap<>(), SILVER_TABLE_PATH2);

        spark.sql("CREATE TABLE IF NOT EXISTS " + SILVER_SQL_TABLE1 +
                "(id bigint, firstName string, lastName string) " +
                "USING iceberg");

        spark.sql("CREATE TABLE IF NOT EXISTS " + SILVER_SQL_TABLE2 +
                "(AddressId bigint, PartyId bigint, " +
                "streetNo int, cityName string, zipcode int, county string) " +
                "USING iceberg");
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

    /*
    Scenario 1: happy path
     */

    @Test
    public void addEntireRecordTest() throws InterruptedException {

        spark.read().format("iceberg").table(SILVER_SQL_TABLE1).show();

        spark.sql("INSERT INTO " + BRONZE_SQL_TABLE + " VALUES " +
                "(3, \'no\', \'one\', 456, \'boston\', 90578, \'usa\', 888, \'san francisco\', 99999, \'usa\', current_timestamp())");

        Thread.sleep(2000);

        spark.read().format("iceberg").table(SILVER_SQL_TABLE1).show();
    }

}


