package org.benchmark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

import ch.cern.sparkmeasure.StageMetrics;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SparkSQLBenchmark {

    private SparkSession spark;
    private StageMetrics stageMetrics;

    // setup and tear down execution time is not included in the benchmark runtime measurements.
    @Setup
    public void setup() {
        SparkConf conf = new SparkConf()
                .setAppName("SparkSQLBenchmark")
                .setMaster("local[1]")
                // 1 executor per instance of each worker
                .set("spark.executor.instances", "1")
                // 4 cores on each executor
                .set("spark.executor.cores", "4")
                .set("log4j.rootCategory=ERROR", "console");

        spark = SparkSession.builder().config(conf).getOrCreate();

        stageMetrics = new StageMetrics(spark);

        String resourcesPath = "src/main/resources/";
        SparkSQL.exportTCPDSData(
                spark,
                resourcesPath + "tpcds_data/call_center/part-00000-52733cca-0604-4ebb-ba8f-612f9aa375ab-c000.snappy.parquet",
                resourcesPath + "tpcds_data/catalog_page/part-00000-4b931ee0-00f2-4b2d-958e-fbdca8b1ed70-c000.snappy.parquet",
                resourcesPath + "tpcds_data/catalog_returns/part-00000-861a1bf0-b23a-4b25-ad8c-45723ac46a5e-c000.snappy.parquet",
                resourcesPath + "tpcds_data/catalog_sales/part-00000-a3c1f01b-fcd7-4187-9ada-ed9163fcf423-c000.snappy.parquet",
                resourcesPath + "tpcds_data/customer/part-00000-b969e874-e1c4-42cc-9ca7-b50b7089ce62-c000.snappy.parquet",
                resourcesPath + "tpcds_data/customer_address/part-00000-2c17ef16-76e4-424e-abb4-0fe79dbbb86e-c000.snappy.parquet",
                resourcesPath + "tpcds_data/customer_demographics/part-00000-530769f4-7165-4749-9de1-e8115a83c77b-c000.snappy.parquet",
                resourcesPath + "tpcds_data/date_dim/part-00000-7c16a98a-ad3d-49ac-823f-27bfc23a121a-c000.snappy.parquet",
                resourcesPath + "tpcds_data/household_demographics/part-00000-9c7cb580-fc86-45cd-a931-4c3a57223a40-c000.snappy.parquet",
                resourcesPath + "tpcds_data/income_band/part-00000-9fa1fc84-dfa6-44bc-92e0-72e4c41cd588-c000.snappy.parquet",
                resourcesPath + "tpcds_data/inventory/part-00000-72d8a3d7-1c35-41ac-b5dc-86880187081c-c000.snappy.parquet",
                resourcesPath + "tpcds_data/item/part-00000-b7cd73d1-96ce-44f5-9184-6c096c6d26d3-c000.snappy.parquet",
                resourcesPath + "tpcds_data/promotion/part-00000-baeeb226-e42e-4cf5-91ba-a4169d65eb41-c000.snappy.parquet",
                resourcesPath + "tpcds_data/reason/part-00000-a055c64e-9f44-41bd-ae2a-c0aa4087e1d2-c000.snappy.parquet",
                resourcesPath + "tpcds_data/ship_mode/part-00000-64e38172-8c78-4ffb-b7ff-cb1471a1a9a5-c000.snappy.parquet",
                resourcesPath + "tpcds_data/store/part-00000-1d5e42f9-92dc-4981-8d8c-5ffe7078abea-c000.snappy.parquet",
                resourcesPath + "tpcds_data/store_returns/part-00000-2c234578-4908-4779-8363-6c2a95160add-c000.snappy.parquet",
                resourcesPath + "tpcds_data/store_sales/part-00000-771c4203-7b85-46c9-90e0-832cd6f94ea8-c000.snappy.parquet",
                resourcesPath + "tpcds_data/time_dim/part-00000-339eb101-10d8-44d6-8d5f-9617c3207194-c000.snappy.parquet",
                resourcesPath + "tpcds_data/warehouse/part-00000-2b78937c-167a-4d13-8e8e-1b0581f190cf-c000.snappy.parquet",
                resourcesPath + "tpcds_data/web_page/part-00000-b1d5c0cf-1a52-4104-856d-0276e928be4d-c000.snappy.parquet",
                resourcesPath + "tpcds_data/web_returns/part-00000-7abdd026-65b2-4fad-876e-d5c7de52ead4-c000.snappy.parquet",
                resourcesPath + "tpcds_data/web_sales/part-00000-25be34eb-a4e9-4150-880a-a138a493b76a-c000.snappy.parquet",
                resourcesPath + "tpcds_data/web_site/part-00000-66725743-da8b-4a38-8205-0531d816b109-c000.snappy.parquet"
        );
    }

    @TearDown
    public void tearDown() {
        spark.stop();
    }

    @Benchmark
    public void coreRunAllQueries() {
        SparkSQL.coreExecuteQueries(spark, 100);
    }

    @Benchmark
    public void pluginRunAllQueries() {
        SparkSQL.pluginExecuteQueries(stageMetrics, spark, 100);
    }

    @Benchmark
    public void coreRunSubsetQueries() {
        SparkSQL.coreExecuteQueries(spark, 50);
    }

    @Benchmark
    public void pluginRunSubsetQueries() {
        SparkSQL.pluginExecuteQueries(stageMetrics, spark, 50);
    }
}
