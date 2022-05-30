import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import com.github.javafaker.Faker;
import models.SampleMultipleFieldsRecord;
import models.SampleThreeFieldRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.junit.*;

public class IcebergSnapshotVersioningTest {
    private static final Configuration CONF = new Configuration();

    private static SparkSession spark = null;

    Faker faker = new Faker();

    @BeforeClass
    public static void startSpark() {
        SparkConf sparkConf = getSparkConfig();
        IcebergSnapshotVersioningTest.spark = SparkSession.builder().master("local[2]").config(sparkConf).getOrCreate();
    }

    @AfterClass
    public static void stopSpark() {
        SparkSession currentSpark = IcebergSnapshotVersioningTest.spark;
        IcebergSnapshotVersioningTest.spark = null;
        currentSpark.stop();
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

    @Test
    public void testSnapshotSelectionBySnapshotId() throws IOException {
        String tableName = faker.name().firstName().toLowerCase();

        String sparkSqlTableLocation = "local.db." + tableName;
        String hadoopTableLocation = "spark-warehouse/db/" + tableName;

        Schema SCHEMA = new Schema(
                Types.NestedField.optional(1, "employeeId", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "baseSalary", Types.DoubleType.get())
        );

        HadoopTables tables = new HadoopTables(CONF);
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = tables.create(SCHEMA, spec, hadoopTableLocation);

        // Generate the first snapshot
        List<SampleThreeFieldRecord> firstBatchRecords = Lists.newArrayList(
                new SampleThreeFieldRecord(1, "Sam", 100000.0),
                new SampleThreeFieldRecord(2, "John", 200000.0),
                new SampleThreeFieldRecord(3, "Peter", 300000.0)
        );
        Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SampleThreeFieldRecord.class);
        firstDf.select("employeeId", "name", "baseSalary").write().format("iceberg").mode("append").save(hadoopTableLocation);

        // Generate the second snapshot
        List<SampleThreeFieldRecord> secondBatchRecords = Lists.newArrayList(
                new SampleThreeFieldRecord(4, "Bob", 150000.0),
                new SampleThreeFieldRecord(5, "Elon", 250000.0),
                new SampleThreeFieldRecord(6, "Mark", 350000.0)
        );
        Dataset<Row> secondDf = spark.createDataFrame(secondBatchRecords, SampleThreeFieldRecord.class);
        secondDf.select("employeeId", "name", "baseSalary").write().format("iceberg").mode("append").save(hadoopTableLocation);

        Assert.assertEquals("Expected 2 snapshots", 2, Iterables.size(table.snapshots()));

        // Verify the records in the current snapshot
        Dataset<Row> currentSnapshotResult = spark.read()
                .format("iceberg")
                .load(hadoopTableLocation);
        currentSnapshotResult.show();

        List<SampleThreeFieldRecord> currentSnapshotRecords = currentSnapshotResult.orderBy("employeeId")
                .as(Encoders.bean(SampleThreeFieldRecord.class))
                .collectAsList();
        List<SampleThreeFieldRecord> expectedRecords = Lists.newArrayList();
        expectedRecords.addAll(firstBatchRecords);
        expectedRecords.addAll(secondBatchRecords);
        Assert.assertEquals("Current snapshot rows should match", expectedRecords, currentSnapshotRecords);

        // Verify records in the previous snapshot
        Snapshot currentSnapshot = table.currentSnapshot();
        Long parentSnapshotId = currentSnapshot.parentId();
        Dataset<Row> previousSnapshotResult = spark.read()
                .format("iceberg")
                .option("snapshot-id", parentSnapshotId)
                .load(hadoopTableLocation);
        List<SampleThreeFieldRecord> previousSnapshotRecords = previousSnapshotResult.orderBy("employeeId")
                .as(Encoders.bean(SampleThreeFieldRecord.class))
                .collectAsList();
        Assert.assertEquals("Previous snapshot rows should match", firstBatchRecords, previousSnapshotRecords);

        // Drop the table
        spark.sql("DROP TABLE IF EXISTS " + sparkSqlTableLocation);
    }

    @Test
    public void testReorderedColumns() throws Exception {
        String tableName = faker.name().firstName().toLowerCase();

        String sparkSqlTableLocation = "local.db." + tableName;
        String hadoopTableLocation = "spark-warehouse/db/" + tableName;

        Schema schema = new Schema(
                Types.NestedField.optional(1, "studentID", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "course", Types.StringType.get()),
                Types.NestedField.optional(4, "gpa", Types.DoubleType.get())
        );

        HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
        Table table = tables.create(schema, PartitionSpec.unpartitioned(), hadoopTableLocation);
        table.updateProperties().set("iceberg.snapshot.versioning.enabled", "true").commit();

        List<SampleMultipleFieldsRecord> batchRecords = Lists.newArrayList(
                new SampleMultipleFieldsRecord(1, "Sam", "Math", 3.78),
                new SampleMultipleFieldsRecord(2, "John", "Physiology", 3.50),
                new SampleMultipleFieldsRecord(3, "Peter", "Arts", 3.8)
        );
        Dataset<Row> dataFrame = spark.createDataFrame(batchRecords, SampleMultipleFieldsRecord.class);

        dataFrame.select("studentID", "name", "course", "gpa")
                .write()
                .format("iceberg")
                .mode("append")
                .save(hadoopTableLocation);

        List<SampleMultipleFieldsRecord> snapshotResult = spark.read()
                .format("iceberg")
                .option("snapshot-id", table.currentSnapshot().snapshotId())
                .load(hadoopTableLocation)
                .orderBy("course")
                .as(Encoders.bean(SampleMultipleFieldsRecord.class))
                .collectAsList();

        List<SampleMultipleFieldsRecord> expectedRecords = batchRecords.stream()
                .sorted(Comparator.comparing(SampleMultipleFieldsRecord::getCourse))
                .collect(Collectors.toList());

        Assert.assertEquals("Expected records should match", expectedRecords, snapshotResult);

        spark.sql("DROP TABLE IF EXISTS " + sparkSqlTableLocation);
    }

    @Test
    public void testSnapshotVersionHistory() throws IOException {
        String tableName = faker.name().firstName().toLowerCase();

        String sparkSqlTableLocation = "local.db." + tableName;
        String hadoopTableLocation = "spark-warehouse/db/" + tableName;
        Schema SCHEMA = new Schema(
                Types.NestedField.optional(1, "employeeId", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "baseSalary", Types.DoubleType.get())
        );

        HadoopTables tables = new HadoopTables(CONF);
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = tables.create(SCHEMA, spec, hadoopTableLocation);

        // Generate 5 snapshots
        for (int i = 0; i < 5; i++) {
            List<SampleThreeFieldRecord> batchRecords = new ArrayList<>();
            for (int j = 0; j < faker.number().numberBetween(15, 30); ++j) {
                batchRecords.add(new SampleThreeFieldRecord(faker.number().randomDigitNotZero(), faker.name().firstName(), faker.number().randomDouble(2, 80000, 200000)));
            }

            Dataset<Row> dataFrame = spark.createDataFrame(batchRecords, SampleThreeFieldRecord.class);
            dataFrame.select("employeeId", "name", "baseSalary").write().format("iceberg").mode("append").save(hadoopTableLocation);
        }

        // Verify the number of snapshots
        Assert.assertEquals("Expected 5 snapshots", 5, Iterables.size(table.snapshots()));

        // Print the version history
        printSnapshotVersionLinkedList(table);

        // Expire second and third snapshots

        int i = 1;
        for (Snapshot snapshot : table.snapshots()) {
            if (i == 2 || i == 3) {
                table.expireSnapshots().expireSnapshotId(snapshot.snapshotId()).commit();
            }

            i++;
        }

        printSnapshotVersionLinkedList(table);

        // Verify the number of snapshots
        Assert.assertEquals("Expected 3 snapshots", 3, Iterables.size(table.snapshots()));

        Dataset<Row> result = spark.sql("SELECT summary FROM " + sparkSqlTableLocation + ".snapshots");
        result.show();

        spark.sql("DROP TABLE IF EXISTS " + sparkSqlTableLocation);
    }

    private void printSnapshotVersionLinkedList(Table table) {
        System.out.println("\n\n\n****************************************************************************************************************************");

        // Print the version history
        for (Snapshot snapshot : table.snapshots()) {
            System.out.print(snapshot.snapshotId() + " -> ");
        }

        System.out.println("\n****************************************************************************************************************************\n\n\n");
        System.out.println();
    }

    @Test(expected = NullPointerException.class)
    public void snapshotVersioningTraversalViaParentIdTest() throws IOException {
        String tableName = faker.name().firstName().toLowerCase();

        String sparkSqlTableLocation = "local.db." + tableName;
        String hadoopTableLocation = "spark-warehouse/db/" + tableName;

        try {
            Schema SCHEMA = new Schema(
                    Types.NestedField.optional(1, "employeeId", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "name", Types.StringType.get()),
                    Types.NestedField.optional(3, "baseSalary", Types.DoubleType.get())
            );

            HadoopTables tables = new HadoopTables(CONF);
            PartitionSpec spec = PartitionSpec.unpartitioned();
            Table table = tables.create(SCHEMA, spec, hadoopTableLocation);

            // Generate 5 snapshots
            for (int i = 0; i < 5; i++) {
                // Generate random names and salaries

                List<SampleThreeFieldRecord> batchRecords = new ArrayList<>();
                for (int j = 0; j < faker.number().numberBetween(15, 30); ++j) {
                    batchRecords.add(new SampleThreeFieldRecord(faker.number().randomDigitNotZero(), faker.name().firstName(), faker.number().randomDouble(2, 80000, 200000)));
                }

                Dataset<Row> dataFrame = spark.createDataFrame(batchRecords, SampleThreeFieldRecord.class);
                dataFrame.select("employeeId", "name", "baseSalary").write().format("iceberg").mode("append").save(hadoopTableLocation);
            }

            // Print the version history
            printSnapshotVersionViaParentIdLinkedList(table);

            // Expire second and third snapshots
            int i = 1;
            for (Snapshot snapshot : table.snapshots()) {
                if (i == 2 || i == 3) {
                    table.expireSnapshots().expireSnapshotId(snapshot.snapshotId()).commit();
                }

                i++;
            }

            printSnapshotVersionViaParentIdLinkedList(table);
        } catch (Exception e) {
            // Drop the table
            spark.sql("DROP TABLE IF EXISTS " + sparkSqlTableLocation);
            throw e;
        }
    }

    private void printSnapshotVersionViaParentIdLinkedList(Table table) {
        Snapshot currentSnapshot = table.currentSnapshot();
        LinkedList<Long> snapshotIdVersions = new LinkedList<>();

        while (currentSnapshot.parentId() != null) {
            snapshotIdVersions.addFirst(currentSnapshot.snapshotId());
            currentSnapshot = table.snapshot(currentSnapshot.parentId());
        }

        snapshotIdVersions.addFirst(currentSnapshot.snapshotId());

        System.out.println("\n\n\n****************************************************************************************************************************");

        // Print the version history
        for (Long snapshotId : snapshotIdVersions) {
            System.out.print(snapshotId + " -> ");
        }

        System.out.println("\n****************************************************************************************************************************\n\n\n");
    }

    @Test
    public void testSnapshotVersionSql() throws IOException {
        String tableName = faker.name().firstName().toLowerCase();

        String sparkSqlTableLocation = "local.db." + tableName;
        String hadoopTableLocation = "spark-warehouse/db/" + tableName;

        Schema SCHEMA = new Schema(
                Types.NestedField.optional(1, "employeeId", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "baseSalary", Types.DoubleType.get())
        );

        HadoopTables tables = new HadoopTables(CONF);

        List<SampleThreeFieldRecord> batchRecords = new ArrayList<>();
        for (int j = 0; j < 5; ++j) {
            batchRecords.add(new SampleThreeFieldRecord(faker.number().randomDigitNotZero(), faker.name().firstName(), faker.number().randomDouble(2, 80000, 200000)));
        }

        Dataset<Row> dataFrame = spark.createDataFrame(batchRecords, SampleThreeFieldRecord.class);

        dataFrame.writeTo(sparkSqlTableLocation).createOrReplace();

        List<Row> results = spark.sql("SELECT * FROM " + sparkSqlTableLocation).collectAsList();

        Assert.assertEquals("Expected 5 rows", 5, results.size());

        Table table = tables.load(hadoopTableLocation);
        Assert.assertEquals("Expected 1 snapshot", 1, Iterables.size(table.snapshots()));

        spark.sql("DROP TABLE IF EXISTS " + sparkSqlTableLocation);
    }

    @Test
    public void testSetCurrentSnapshotToParentSnapshot() {
        String tableName = faker.name().firstName().toLowerCase();

        String sparkSqlTableLocation = "local.db." + tableName;
        String hadoopTableLocation = "spark-warehouse/db/" + tableName;

        Schema SCHEMA = new Schema(
                Types.NestedField.optional(1, "employeeId", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "baseSalary", Types.DoubleType.get())
        );

        PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
                .bucket("employeeId", 4)
                .build();

        int numRecords = 10;
        List<SampleThreeFieldRecord> batchRecords = Lists.newArrayList(
                new SampleThreeFieldRecord(1, "Sam", 100000.0),
                new SampleThreeFieldRecord(2, "John", 200000.0),
                new SampleThreeFieldRecord(3, "Peter", 300000.0),
                new SampleThreeFieldRecord(4, "Bob", 150000.0),
                new SampleThreeFieldRecord(5, "David", 120000.0)
        );

        HadoopTables tables = new HadoopTables(CONF);
        Dataset<Row> dataFrame = spark.createDataFrame(batchRecords, SampleThreeFieldRecord.class);

        dataFrame.writeTo(sparkSqlTableLocation).createOrReplace();

        List<Row> sparkSqlResult1 = spark.sql("SELECT * FROM " + sparkSqlTableLocation).collectAsList();
        Assert.assertEquals("Expected 5 rows", 5, sparkSqlResult1.size());

        // Delete from the table where baseSalary > 180000
        spark.sql("DELETE FROM " + sparkSqlTableLocation + " WHERE baseSalary > 180000").show();

        List<Row> sparkSqlResult2 = spark.sql("SELECT * FROM " + sparkSqlTableLocation).collectAsList();
        Assert.assertEquals("Expected 3 rows", 3, sparkSqlResult2.size());

        Table table = tables.load(hadoopTableLocation);

        // Set current snapshot to the parent of the current snapshot
        table.manageSnapshots().setCurrentSnapshot(table.currentSnapshot().parentId()).commit();

        Dataset<Row> currentSnapshotResult = spark.read()
                .format("iceberg")
                .load(hadoopTableLocation);

        // Now the current snapshot spark.read() should return 5 rows
        Assert.assertEquals("Expected 5 rows", 5, currentSnapshotResult.count());

        // However, Spark sql results still have 3 rows even though the current snapshot has 5 rows.
        List<Row> sparkSqlResult3 = spark.sql("SELECT * FROM " + sparkSqlTableLocation).collectAsList();
        Assert.assertEquals("Expected 3 rows", 3, sparkSqlResult3.size());

        // Drop the table in the end
        spark.sql("DROP TABLE IF EXISTS " + sparkSqlTableLocation);
    }
}
