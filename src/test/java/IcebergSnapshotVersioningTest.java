import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import models.SampleMultipleFieldsRecord;
import models.SampleThreeFieldRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.xml.crypto.Data;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class IcebergSnapshotVersioningTest {
    private static final Configuration CONF = new Configuration();

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private static SparkSession spark = null;

    @BeforeClass
    public static void startSpark() {
        IcebergSnapshotVersioningTest.spark = SparkSession.builder().master("local[2]").getOrCreate();
    }

    @AfterClass
    public static void stopSpark() {
        SparkSession currentSpark = IcebergSnapshotVersioningTest.spark;
        IcebergSnapshotVersioningTest.spark = null;
        currentSpark.stop();
    }

    @Test
    public void testSnapshotSelectionBySnapshotId() throws IOException {
        String tableLocation = temp.newFolder("iceberg-table").toString();

        Schema SCHEMA = new Schema(
                optional(1, "employeeId", Types.IntegerType.get()),
                optional(2, "name", Types.StringType.get()),
                optional(3, "baseSalary", Types.DoubleType.get())

        );

        HadoopTables tables = new HadoopTables(CONF);
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = tables.create(SCHEMA, spec, tableLocation);

        // Generate the first snapshot
        List<SampleThreeFieldRecord> firstBatchRecords = Lists.newArrayList(
                new SampleThreeFieldRecord(1, "Sam", 100000.0),
                new SampleThreeFieldRecord(2, "John", 200000.0),
                new SampleThreeFieldRecord(3, "Peter", 300000.0)
        );
        Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SampleThreeFieldRecord.class);
        firstDf.select("employeeId", "name", "baseSalary").write().format("iceberg").mode("append").save(tableLocation);

        // Generate the second snapshot
        List<SampleThreeFieldRecord> secondBatchRecords = Lists.newArrayList(
                new SampleThreeFieldRecord(4, "Bob", 150000.0),
                new SampleThreeFieldRecord(5, "Elon", 250000.0),
                new SampleThreeFieldRecord(6, "Mark", 350000.0)
        );
        Dataset<Row> secondDf = spark.createDataFrame(secondBatchRecords, SampleThreeFieldRecord.class);
        secondDf.select("employeeId", "name", "baseSalary").write().format("iceberg").mode("append").save(tableLocation);

        Assert.assertEquals("Expected 2 snapshots", 2, Iterables.size(table.snapshots()));

        // Verify the records in the current snapshot
        Dataset<Row> currentSnapshotResult = spark.read()
                .format("iceberg")
                .load(tableLocation);
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
                .load(tableLocation);
        List<SampleThreeFieldRecord> previousSnapshotRecords = previousSnapshotResult.orderBy("employeeId")
                .as(Encoders.bean(SampleThreeFieldRecord.class))
                .collectAsList();
        Assert.assertEquals("Previous snapshot rows should match", firstBatchRecords, previousSnapshotRecords);
    }

    @Test
    public void testReorderedColumns() throws Exception {
        String tableLocation = temp.newFolder("reorder_columns").toString();

        Schema schema = new Schema(
                optional(1, "studentID", Types.IntegerType.get()),
                optional(2, "name", Types.StringType.get()),
                optional(3, "course", Types.StringType.get()),
                optional(4, "gpa", Types.DoubleType.get())
        );

        HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
        Table table = tables.create(schema, PartitionSpec.unpartitioned(), tableLocation);
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
                .save(tableLocation);

        List<SampleMultipleFieldsRecord> snapshotResult = spark.read()
                .format("iceberg")
                .option("snapshot-id", table.currentSnapshot().snapshotId())
                .load(tableLocation)
                .orderBy("course")
                .as(Encoders.bean(SampleMultipleFieldsRecord.class))
                .collectAsList();

        List<SampleMultipleFieldsRecord> expectedRecords = batchRecords.stream()
                .sorted(Comparator.comparing(SampleMultipleFieldsRecord::getCourse))
                .collect(Collectors.toList());

        Assert.assertEquals("Expected records should match", expectedRecords, snapshotResult);
    }
}
