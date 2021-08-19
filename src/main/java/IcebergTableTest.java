import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IcebergTableTest {
    static private String SAMPLE_TABLE_NAME = "local.db.table";
    static private String TARGET_TABLE_PATH = String.format("%s/%s", "spark-warehouse", SAMPLE_TABLE_NAME);

    public static void main(String[] args) {
        SparkConf config = getSparkConfig();
        SparkSession spark = SparkSession
                .builder()
                .appName("icebergTesting")
                .master("local")
                .config(config)
                .getOrCreate();

        String path = "src/main/resources/sample.csv";
        Dataset<Row> df = spark.read().format("iceberg")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);

        df.writeTo(SAMPLE_TABLE_NAME).createOrReplace();
        df.show();
        df = spark.table(SAMPLE_TABLE_NAME);
        //df = spark.read().format("iceberg").load(SAMPLE_TABLE_NAME);
        df.show();
    }

    static private SparkConf getSparkConfig() {
        SparkConf config = new SparkConf();
        config.set("spark.sql.legacy.createHiveTableByDefault", "false");
        config.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        config.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
        config.set("spark.sql.catalog.spark_catalog.type", "hive");
        config.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog");
        config.set("spark.sql.catalog.local.type", "hadoop");
        config.set("spark.sql.catalog.local.warehouse", "spark-warehouse");
        return config;
    }
}
