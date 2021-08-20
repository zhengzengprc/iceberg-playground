import static org.apache.spark.sql.functions.lit;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class IcebergIngestionTest {
    static private String TEST_FILE_DIR_PATH = "src/main/resources/csvtests";
    private SparkSession spark;

    @Before
    public void setUp() throws Exception {
        SparkConf config = getSparkConfig();
        spark = SparkSession
                .builder()
                .appName("icebergTesting")
                .master("local")
                .config(config)
                .getOrCreate();
    }

    @Test
    public void testIcebergReadFromCSVSuccessful() {
        final String targetDBName = "local.db.individual";
        final String path = TEST_FILE_DIR_PATH + "/individual.csv";
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);

        df.writeTo(targetDBName).createOrReplace();
        df = spark.table(targetDBName);
        assertEquals(2, df.count());
    }

    @Test
    public void testIcebergReadFromCSVAddNewFieldInTheMiddleSuccessful() throws NoSuchTableException {
        final String targetDBName = "local.db.individualnewfield";
        final String hadoopTablePath = "spark-warehouse/db/individualnewfield";
        final String baseFilePath = TEST_FILE_DIR_PATH + "/individual.csv";
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(baseFilePath);
        df.writeTo(targetDBName).createOrReplace();
        df = spark.table(targetDBName);
        assertEquals(2, df.count());
        Table table = loadHadoopTable(hadoopTablePath);
        UpdateSchema updateSchema = table.updateSchema();

        // A new session read did the magic?
        spark = spark.newSession();

        // Add new field in the middle
        final String updatedFilePath = TEST_FILE_DIR_PATH + "/individual_new_field.csv";
        Dataset<Row> df2 = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(updatedFilePath);

        // Update Existing Schema
        Arrays.stream(df2.schema().fields())
                .filter(x -> !table.schema().columns().stream().anyMatch(y -> x.name().equals(y.name())))
                .forEach(z -> updateSchema.addColumn(z.name(), new Types.StringType()));
        updateSchema.commit();

        // In Dataframe add new column
        for (Types.NestedField field : table.schema().columns()) {
            if (Arrays.stream(df2.schema().fields()).anyMatch(y -> field.name().equals(y.name()))) {
                continue;
            }

            df2 = df2.withColumn(
                    field.name(),
                    lit(null).cast(SparkSchemaUtil.convert(table.schema().findType(field.name()))));
        }

        df2.writeTo(targetDBName).append();
        df2 = spark.table(targetDBName);
        df2.show();
        assertEquals(5, df2.count());
    }

    @Test
    public void testIcebergReadFromCSVOrderChangeSuccessful() throws NoSuchTableException {
        final String targetDBName = "local.db.individualorderchange";
        final String baseFilePath = TEST_FILE_DIR_PATH + "/individual.csv";
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(baseFilePath);
        df.writeTo(targetDBName).createOrReplace();
        df = spark.table(targetDBName);
        assertEquals(2, df.count());

        // Read another csv file with same fields but different order
        final String updatedFilePath = TEST_FILE_DIR_PATH + "/individual_order_change.csv";
        Dataset<Row> df2 = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(updatedFilePath);

        df2.writeTo(targetDBName).append();
        df2 = spark.table(targetDBName);
        df2.show();
        assertEquals(4, df2.count());
    }

    @Test
    public void testIcebergReadFromCSVDeleteFieldSuccessful() throws NoSuchTableException {
        final String targetDBName = "local.db.individualdeletefield";
        final String hadoopTablePath = "spark-warehouse/db/individualdeletefield";
        final String baseFilePath = TEST_FILE_DIR_PATH + "/individual.csv";
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(baseFilePath);
        df.writeTo(targetDBName).createOrReplace();
        df = spark.table(targetDBName);
        assertEquals(2, df.count());
        Table table = loadHadoopTable(hadoopTablePath);
        UpdateSchema updateSchema = table.updateSchema();

        // Add new field in the middle
        final String updatedFilePath = TEST_FILE_DIR_PATH + "/individual_delete_field.csv";
        Dataset<Row> df2 = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(updatedFilePath);

        // Update Existing Schema
        Arrays.stream(df2.schema().fields())
                .filter(x -> !table.schema().columns().stream().anyMatch(y -> x.name().equals(y.name())))
                .forEach(z -> updateSchema.addColumn(z.name(), new Types.StringType()));
        updateSchema.commit();

        // In Dataframe add new column
        for (Types.NestedField field : table.schema().columns()) {
            if (Arrays.stream(df2.schema().fields()).anyMatch(y -> field.name().equals(y.name()))) {
                continue;
            }

            df2 = df2.withColumn(
                    field.name(),
                    lit(null).cast(SparkSchemaUtil.convert(table.schema().findType(field.name()))));
        }

        df2.writeTo(targetDBName).append();
        df2 = spark.table(targetDBName);
        df2.show();
        assertEquals(4, df2.count());
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
}