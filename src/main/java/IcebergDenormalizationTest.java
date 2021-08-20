import org.apache.spark.SparkConf;
import org.junit.*;
import org.apache.iceberg.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IcebergDenormalizationTest {
    private static SparkSession spark;
    private static SparkConf sparkConf;
    public static Dataset<Row> bronzeTable;
    /*
    Create initial source Bronze table.
    Create initial target Silver tables (empty).
    Test different denormalization scenarios.
     */


    /*
    First, create source Bronze table.
     */
    @BeforeClass
    public static void setup() {
        setSparkConf();
        setSparkSession();
        createBronzeTable();
    }

    @AfterClass
    public static void tearDown() {
        spark.sql("DROP TABLE IF EXISTS local.db.bronze_table");
    }

    private static void setSparkConf() {
        sparkConf = new SparkConf();
        sparkConf.set("spark.master", "local"); // not running in cluster mode
        sparkConf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
        sparkConf.set("spark.sql.catalog.spark_catalog.type", "hive");
        sparkConf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog");
        sparkConf.set("spark.sql.catalog.local.type", "hadoop");
        sparkConf.set("spark.sql.catalog.local.warehouse", "spark-warehouse");
    }

    private static void setSparkSession() {
        spark = SparkSession
                .builder()
//                .master("local")
                .appName("Denormalization Example")
                .config(sparkConf)
                .getOrCreate();
    }

    private static void createBronzeTable() {
        bronzeTable = spark.sql("CREATE TABLE IF NOT EXISTS local.db.bronze_table " +
                "(id bigint, firstName string, lastName string," +
                "streetNo1 int, cityName1 string, zipcode1 int, county1 string," +
                "streetNo2 int, cityName2 string, zipcode2 int, county2 string) " +
                "USING iceberg");
        bronzeTable = spark.sql("INSERT INTO local.db.bronze_table VALUES " +
                "(1, \'abc\', \'bcd\', 123, \'redmond\', 98022, \'usa\', 343, \'bellevue\', 98077, \'usa\')," +
                "(2, \'some\', \'one\', 444, \'seattle\', 98008, \'usa\', NULL, NULL, NULL, NULL)");
    }



    /*
    Scenario 1: happy path
     */

    @Test
    public void sampleTest() {
        spark.sql("SELECT * FROM local.db.bronze_table").show();
        spark.sql("SELECT * FROM local.db.bronze_table.snapshots").show();
        TableScan scan = bronzeTable.newScan()
    }


}
