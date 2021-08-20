import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
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

import java.util.HashMap;
import java.util.Map;

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
        createBronzeTable();
    }

    @AfterClass
    public static void tearDown() {
        bronzeCatalog.dropTable(bronzeTableId);
    }


    private static void createBronzeTable() {
        bronzeCatalog = new HadoopCatalog(new Configuration(), "warehouse");
        bronzeTableId = TableIdentifier.of("db", "bronze_table");
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
                .identity("id")
                .build();
        bronzeTable = bronzeCatalog.createTable(bronzeTableId, bronzeSchema, bronzeSpec);
    }

    /*
    Scenario 1: happy path
     */

    @Test
    public void sampleTest() {
    }


}

