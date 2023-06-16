package org.benchmark;

import ch.cern.sparkmeasure.StageMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class SparkSQL {

    public static void exportTCPDSData(
            SparkSession spark,
            String parquetFilepath1,
            String parquetFilepath2,
            String parquetFilepath3,
            String parquetFilepath4,
            String parquetFilepath5,
            String parquetFilepath6,
            String parquetFilepath7,
            String parquetFilepath8,
            String parquetFilepath9,
            String parquetFilepath10,
            String parquetFilepath11,
            String parquetFilepath12,
            String parquetFilepath13,
            String parquetFilepath14,
            String parquetFilepath15,
            String parquetFilepath16,
            String parquetFilepath17,
            String parquetFilepath18,
            String parquetFilepath19,
            String parquetFilepath20,
            String parquetFilepath21,
            String parquetFilepath22,
            String parquetFilepath23,
            String parquetFilepath24
    ) {
        // Read the Parquet files into a DataFrame
        Dataset<Row> df1 = spark.read().parquet(parquetFilepath1);
        Dataset<Row> df2 = spark.read().parquet(parquetFilepath2);
        Dataset<Row> df3 = spark.read().parquet(parquetFilepath3);
        Dataset<Row> df4 = spark.read().parquet(parquetFilepath4);
        Dataset<Row> df5 = spark.read().parquet(parquetFilepath5);
        Dataset<Row> df6 = spark.read().parquet(parquetFilepath6);
        Dataset<Row> df7 = spark.read().parquet(parquetFilepath7);
        Dataset<Row> df8 = spark.read().parquet(parquetFilepath8);
        Dataset<Row> df9 = spark.read().parquet(parquetFilepath9);
        Dataset<Row> df10 = spark.read().parquet(parquetFilepath10);
        Dataset<Row> df11 = spark.read().parquet(parquetFilepath11);
        Dataset<Row> df12 = spark.read().parquet(parquetFilepath12);
        Dataset<Row> df13 = spark.read().parquet(parquetFilepath13);
        Dataset<Row> df14 = spark.read().parquet(parquetFilepath14);
        Dataset<Row> df15 = spark.read().parquet(parquetFilepath15);
        Dataset<Row> df16 = spark.read().parquet(parquetFilepath16);
        Dataset<Row> df17 = spark.read().parquet(parquetFilepath17);
        Dataset<Row> df18 = spark.read().parquet(parquetFilepath18);
        Dataset<Row> df19 = spark.read().parquet(parquetFilepath19);
        Dataset<Row> df20 = spark.read().parquet(parquetFilepath20);
        Dataset<Row> df21 = spark.read().parquet(parquetFilepath21);
        Dataset<Row> df22 = spark.read().parquet(parquetFilepath22);
        Dataset<Row> df23 = spark.read().parquet(parquetFilepath23);
        Dataset<Row> df24 = spark.read().parquet(parquetFilepath24);

        df1.createOrReplaceTempView("call_center");
        df2.createOrReplaceTempView("catalog_page");
        df3.createOrReplaceTempView("catalog_returns");
        df4.createOrReplaceTempView("catalog_sales");
        df5.createOrReplaceTempView("customer");
        df6.createOrReplaceTempView("customer_address");
        df7.createOrReplaceTempView("customer_demographics");
        df8.createOrReplaceTempView("date_dim");
        df9.createOrReplaceTempView("household_demographics");
        df10.createOrReplaceTempView("income_band");
        df11.createOrReplaceTempView("inventory");
        df12.createOrReplaceTempView("item");
        df13.createOrReplaceTempView("promotion");
        df14.createOrReplaceTempView("reason");
        df15.createOrReplaceTempView("ship_mode");
        df16.createOrReplaceTempView("store");
        df17.createOrReplaceTempView("store_returns");
        df18.createOrReplaceTempView("store_sales");
        df19.createOrReplaceTempView("time_dim");
        df20.createOrReplaceTempView("warehouse");
        df21.createOrReplaceTempView("web_page");
        df22.createOrReplaceTempView("web_returns");
        df23.createOrReplaceTempView("web_sales");
        df24.createOrReplaceTempView("web_site");
    }

    public static String getSQLQuery(String filepath) throws IOException {
        return Files.lines(Paths.get(filepath), StandardCharsets.UTF_8)
                .filter(line -> !line.startsWith("--")) // exclude SQL comments
                .filter(line -> !line.isBlank()) // exclude empty lines
                .map(String::trim)
                .collect(Collectors.joining(" "));
    }

    public static void pluginExecuteQueries(StageMetrics stageMetrics, SparkSession spark, int x) {
        for (int i = 1; i<x; i++) {
            try {
                String query = getSQLQuery("src/main/resources/sql_files/query" +  i + ".sql");
                stageMetrics.runAndMeasure(() -> spark.sql(query).showString(1, 0, false));
                System.out.println("Query " + i + " succeeded");
            } catch (Exception ignored) {
            }
        }
    }

    public static void coreExecuteQueries(SparkSession spark, int x) {
        for (int i = 1; i<x; i++) {
            try {
                String query = getSQLQuery("src/main/resources/sql_files/query" +  i + ".sql");
                spark.sql(query).showString(1, 0, false);
                System.out.println("Query " + i + " succeeded");
            } catch (Exception ignored) {
            }
        }
    }
}
