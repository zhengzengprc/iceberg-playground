import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.types.Types;
import org.apache.kerby.config.Conf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.internal.config.ConfigEntry;
import org.apache.spark.internal.config.ConfigReader;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.*;
import org.apache.iceberg.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;


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
    private static final String SILVER_TALBE_PATH2 = WAREHOUSE + "." + SILVER_SQL_TABLE2;


    @BeforeClass
    public static void setup() {
        setSparkConf();
        setSparkSession();
    }

    /*
    Create source Bronze table.
    Create skeleton Silver tables.
     */
    @Before
    public void setupTables() {
        createBronzeTable();
        createSilverTables();
    }

    /*
    Delete all tables.
     */
    @After
    public void tearDown() {
        bronzeCatalog.dropTable(bronzeTableId);
        silverCatalog1.dropTable(silverTableId1);
        silverCatalog2.dropTable(silverTableId2);
    }

    private static void setSparkSession() {
        spark = SparkSession
                .builder()
                .appName("Denormalization Example")
                .master("local")
                .config(sparkConf)
                .getOrCreate();
        sparkContext = new JavaSparkContext(spark.sparkContext());

    }

    private static void setSparkConf() {
        sparkConf = new SparkConf();
        // spark master URL for distributed cluster: run locally with 1 thread
//        sparkConf.set("spark.master", CATALOG);

        // spark catalog: for non-iceberg tables
//        sparkConf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
//        sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
//        sparkConf.set("spark.sql.catalog.spark_catalog.type", "hive");

        // local catalog: directory-based in HDFS, for iceberg tables
        sparkConf.set("spark.sql.catalog." + CATALOG, "org.apache.iceberg.spark.SparkCatalog");
        sparkConf.set("spark.sql.catalog." + CATALOG + ".type", "hadoop");
        sparkConf.set("spark.sql.catalog." + CATALOG + ".warehouse", WAREHOUSE);
    }

    private static void createSparkBronzeTable() {
        bronzeSparkDataset = spark.sql("CREATE TABLE IF NOT EXISTS " + BRONZE_SQL_TABLE +
                "(id bigint, firstName string, lastName string," +
                "streetNo1 int, cityName1 string, zipcode1 int, county1 string," +
                "streetNo2 int, cityName2 string, zipcode2 int, county2 string) " +
                "USING iceberg");
        bronzeSparkDataset = spark.sql("INSERT INTO " + BRONZE_SQL_TABLE + " VALUES " +
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

        PartitionSpec bronzeSpec = PartitionSpec.builderFor(bronzeSchema)
                .bucket("id",100)
                .build();

        // Catalog method of creating Iceberg table
        bronzeCatalog = new HadoopCatalog(new Configuration(), WAREHOUSE);
        bronzeTableId = TableIdentifier.of(BRONZE_NAMESPACE, BRONZE_TABLE_NAME);
        bronzeTable = bronzeCatalog.createTable(bronzeTableId, bronzeSchema, bronzeSpec);

        createSparkBronzeTable();

        spark.read().format("iceberg").load(BRONZE_SQL_TABLE).show();

//         Table interface method of creating Iceberg table
//        bronzeTable = new HadoopTables().create(bronzeSchema, BRONZE_NAMESPACE + "." + BRONZE_TABLE_NAME);
    }

    private static void createSparkSilverTables() {
        silverSparkDataset1 = spark.sql("CREATE TABLE IF NOT EXISTS " + SILVER_SQL_TABLE1 +
                "(id bigint, firstName string, lastName string) " +
                "USING iceberg");
        silverSparkDataset1 = spark.sql("INSERT INTO " + SILVER_SQL_TABLE1 + " VALUES " +
                "(1, \'abc\', \'bcd\')," +
                "(2, \'some\', \'one\')");

        silverSparkDataset2 = spark.sql("CREATE TABLE IF NOT EXISTS " + SILVER_SQL_TABLE2 +
                "(AddressId bigint, PartyId bigint, " +
                "streetNo int, cityName string, zipcode int, county string) " +
                "USING iceberg");
        silverSparkDataset2 = spark.sql("INSERT INTO " + SILVER_SQL_TABLE2 + " VALUES " +
                "(1, 1, 123, \'redmond\', 98022, \'usa\'), (2, 1, 343, \'bellevue\', 98077, \'usa\')," +
                "(2, 1, 444, \'seattle\', 98008, \'usa\')");
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
                Types.NestedField.optional(4, "streetNo", Types.IntegerType.get()),
                Types.NestedField.optional(5, "cityName", Types.StringType.get()),
                Types.NestedField.optional(6, "zipcode", Types.IntegerType.get()),
                Types.NestedField.optional(7, "county", Types.StringType.get())
        );

        PartitionSpec silverSpec1 = PartitionSpec.builderFor(silverSchema1)
                .identity("id")
                .bucket("id",100)
                .build();

        PartitionSpec silverSpec2 = PartitionSpec.builderFor(silverSchema2)
                .bucket("PartyId",100)
                .build();

        // Catalog method of creating Iceberg table
        silverCatalog1 = new HadoopCatalog(new Configuration(), WAREHOUSE);
        silverTableId1 = TableIdentifier.of(SILVER_NAMESPACE, SILVER_TABLE_NAME1);
        silverTable1 = silverCatalog1.createTable(silverTableId1, silverSchema1, silverSpec1);
        silverCatalog2 = new HadoopCatalog(new Configuration(), WAREHOUSE);
        silverTableId2 = TableIdentifier.of(SILVER_NAMESPACE, SILVER_TABLE_NAME2);
        silverTable2 = silverCatalog2.createTable(silverTableId2, silverSchema2, silverSpec2);

        createSparkSilverTables();

        spark.read().format("iceberg").load(SILVER_SQL_TABLE1).show();
        spark.read().format("iceberg").load(SILVER_SQL_TABLE2).show();
    }

    /*
    Scenario 1: happy path
     */

    @Test
    /*
    Given:
        - schemas of the bronze and silver tables
        - mapping between bronze and silver columns


     */
    public void addEntireRecordTest() {
    }


}

