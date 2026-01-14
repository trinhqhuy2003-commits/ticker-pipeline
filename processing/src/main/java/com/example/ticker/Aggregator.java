package com.example.ticker;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class Aggregator {

        public static void main(String[] args)
                        throws TimeoutException, org.apache.spark.sql.streaming.StreamingQueryException {
                System.out.println("Starting Spark Ticker Aggregator...");

                String kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS",
                                "localhost:9092");
                String topic = "ticker_prices";
                String postgresUrl = System.getenv().getOrDefault("POSTGRES_URL",
                                "jdbc:postgresql://localhost:5432/ticker_db");
                String postgresUser = System.getenv().getOrDefault("POSTGRES_USER", "user");
                String postgresPassword = System.getenv().getOrDefault("POSTGRES_PASSWORD", "password");

                SparkSession spark = SparkSession
                                .builder()
                                .appName("TickerAggregator")
                                .master("local[*]") // In production/cluster, this would be set via spark-submit
                                .getOrCreate();

                spark.sparkContext().setLogLevel("WARN");

                // Define schema for JSON data
                StructType schema = new StructType()
                                .add("symbol", DataTypes.StringType)
                                .add("price", DataTypes.DoubleType)
                                .add("timestamp", DataTypes.LongType);

                // Read from Kafka
                Dataset<Row> df = spark
                                .readStream()
                                .format("kafka")
                                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                                .option("subscribe", topic)
                                .option("startingOffsets", "latest")
                                .load();

                // Parse JSON and convert timestamp
                Dataset<Row> parsedDf = df.selectExpr("CAST(value AS STRING)")
                                .select(functions.from_json(functions.col("value"), schema).as("data"))
                                .select("data.*")
                                .withColumn("timestamp",
                                                functions.to_timestamp(functions.col("timestamp").divide(1000)));

                // Aggregate 5-minute tumbling window
                Dataset<Row> aggregatedDf = parsedDf
                                .groupBy(
                                                functions.window(functions.col("timestamp"), "5 minutes"),
                                                functions.col("symbol"))
                                .agg(functions.avg("price").as("avg_price"))
                                .select(
                                                functions.col("symbol"),
                                                functions.col("window.start").as("window_start"),
                                                functions.col("window.end").as("window_end"),
                                                functions.col("avg_price"));

                // Write content to Postgres
                StreamingQuery query = aggregatedDf.writeStream()
                                .outputMode("update")
                                .foreachBatch((batchDf, batchId) -> {
                                        batchDf.write()
                                                        .mode(SaveMode.Append)
                                                        .format("jdbc")
                                                        .option("url", postgresUrl)
                                                        .option("dbtable", "price_aggregates")
                                                        .option("user", postgresUser)
                                                        .option("password", postgresPassword)
                                                        .option("driver", "org.postgresql.Driver")
                                                        .save();
                                })
                                .start();

                query.awaitTermination();
        }
}
