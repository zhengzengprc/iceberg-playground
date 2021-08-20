import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.types.Types;
import org.apache.kerby.config.Conf;
import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.ConfigEntry;
import org.apache.spark.internal.config.ConfigReader;
import org.junit.*;
import org.apache.iceberg.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/*
Create initial source Bronze table.
Create initial target Silver tables (empty).
Test different denormalization scenarios.
 */
@RunWith(JUnit4.class)
public class IcebergDenormalizationTest {
    private static SparkSession spark;
    private static SparkConf sparkConf;
    private static Dataset<Row> bronzeSparkDataset;
    private static Table bronzeTable;
    private static Catalog bronzeCatalog;
    private static TableIdentifier bronzeTableId;

    /*
    First, create source Bronze table.
     */
    @BeforeClass
    public static void setup() {
        setSparkConf();
        setSparkSession();
        createSparkBronzeTable();
//        createBronzeTable();
    }

    @AfterClass
    public static void tearDown() {
        spark.sql("DROP TABLE IF EXISTS local.db.bronze_table");
    }

    private static void setSparkSession() {
        spark = SparkSession
                .builder()
                .appName("Denormalization Example")
                .config(sparkConf)
                .getOrCreate();
    }

    private static void setSparkConf() {
        sparkConf = new SparkConf();
        // spark master URL for distrubted cluster: run locally with 1 thread
        sparkConf.set("spark.master", "local");

        // spark catalog: for non-iceberg tables
        sparkConf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
        sparkConf.set("spark.sql.catalog.spark_catalog.type", "hive");

        // local catalog: directory-based in HDFS, for iceberg tables
        sparkConf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog");
        sparkConf.set("spark.sql.catalog.local.type", "hadoop");
        sparkConf.set("spark.sql.catalog.local.warehouse", "warehouse");

    }

    private static void createSparkBronzeTable() {
        bronzeSparkDataset = spark.sql("CREATE TABLE IF NOT EXISTS local.db.bronze_table " +
                "(id bigint, firstName string, lastName string," +
                "streetNo1 int, cityName1 string, zipcode1 int, county1 string," +
                "streetNo2 int, cityName2 string, zipcode2 int, county2 string) " +
                "USING iceberg");
        bronzeSparkDataset = spark.sql("INSERT INTO local.db.bronze_table VALUES " +
                "(1, \'abc\', \'bcd\', 123, \'redmond\', 98022, \'usa\', 343, \'bellevue\', 98077, \'usa\')," +
                "(2, \'some\', \'one\', 444, \'seattle\', 98008, \'usa\', NULL, NULL, NULL, NULL)");
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
                Types.NestedField.optional(11, "county2", Types.StringType.get())
        );
//        PartitionSpec bronzeSpec = PartitionSpec.builderFor(bronzeSchema)
//                .identity("id")
//                .build();

        // Catalog method of creating Iceberg table
//        bronzeCatalog = new HadoopCatalog(new Configuration(), "warehouse");
//        bronzeTableId = TableIdentifier.of("db", "bronze_table");
//        bronzeTable = bronzeCatalog.createTable(bronzeTableId, bronzeSchema, bronzeSpec);

        // Table interface method of creating Iceberg table
        bronzeTable = new HadoopTables().create(bronzeSchema, "db.bronze_table");
    }

    /*
    Scenario 1: happy path
     */

    @Test
    public void sampleTest() {
        spark.sql("SHOW TABLES").show();
        spark.sql("SELECT * FROM local.db.bronze_table").show();
    }


}

