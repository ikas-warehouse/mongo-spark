/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.mongodb.spark.sql.connector.write;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;
import org.bson.BsonNull;

import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.TimeSeriesGranularity;
import com.mongodb.client.model.TimeSeriesOptions;

import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;

import com.google.common.collect.ImmutableList;

class MongoSparkConnectorWriteTest extends MongoSparkConnectorTestCase {

  private static final String TIMESERIES_RESOURCES_JSON_PATH =
      "src/integrationTest/resources/data/timeseries/*.json";

  private static final String WRITE_RESOURCES_JSON_PATH =
      "src/integrationTest/resources/data/write/*.json";

  private static final String WRITE_RESOURCES_CSV_PATH =
      "src/integrationTest/resources/data/write/*.csv";

  @Test
  void testDataType() {
    SparkSession spark = getOrCreateSparkSession();
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("name", DataTypes.StringType, false, Metadata.empty()),
              new StructField("Deci", DataTypes.createDecimalType(27, 9), false, Metadata.empty()),
              new StructField("age", DataTypes.createDecimalType(38, 9), false, Metadata.empty())
            });
    Row r1 =
        RowFactory.create(
            "name1",
            new BigDecimal("123456789012345678.123456789"),
            new BigDecimal("1234567890123456789012345678.123456000"));
    Row r2 =
        RowFactory.create(
            "name2",
            new BigDecimal("123456789012345678.123456789"),
            new BigDecimal("1234567890123456789012345678.123456"));
    List<Row> rowList = ImmutableList.of(r1, r2);
    Dataset<Row> df = spark.sqlContext().createDataFrame(rowList, schema);

    String collectionName = "test";
    DataFrameWriter<Row> dfw =
        df.write().format("mongodb").option(WriteConfig.COLLECTION_NAME_CONFIG, collectionName);

    dfw.mode("Overwrite").save();
    assertEquals(2, getCollection(collectionName).countDocuments());
  }

  @Test
  void testSupportedWriteModes() {
    SparkSession spark = getOrCreateSparkSession();

    Dataset<Row> df = spark.read().json(WRITE_RESOURCES_JSON_PATH);
    DataFrameWriter<Row> dfw = df.write().format("mongodb");

    dfw.mode("Overwrite").save();
    assertEquals(10, getCollection().countDocuments());

    dfw.mode("Append").save();
    assertEquals(20, getCollection().countDocuments());

    dfw.mode("Overwrite").save();
    assertEquals(10, getCollection().countDocuments());

    assertCollection();
  }

  @Test
  void testTimeseriesSupport() {
    assumeTrue(isAtLeastFiveDotZero());
    SparkSession spark = getOrCreateSparkSession();

    getDatabase()
        .createCollection(
            getCollectionName(),
            new CreateCollectionOptions()
                .timeSeriesOptions(
                    new TimeSeriesOptions("timestamp")
                        .metaField("metadata")
                        .granularity(TimeSeriesGranularity.HOURS)));

    StructType schema =
        createStructType(
            asList(
                createStructField(
                    "metadata",
                    DataTypes.createStructType(
                        asList(
                            createStructField("sensorId", DataTypes.IntegerType, false),
                            createStructField("type", DataTypes.StringType, false))),
                    false),
                createStructField("timestamp", DataTypes.DateType, false),
                createStructField("temp", DataTypes.IntegerType, false)));

    Dataset<Row> df = spark.read().schema(schema).json(TIMESERIES_RESOURCES_JSON_PATH);
    df.write()
        .mode("Append")
        .format("mongodb")
        .option(WriteConfig.UPSERT_DOCUMENT_CONFIG, "false")
        .save();

    assertEquals(
        1,
        getDatabase()
            .listCollections()
            .filter(
                BsonDocument.parse(
                    "{ \"name\": \"" + getCollectionName() + "\",  \"type\": \"timeseries\"}"))
            .into(new ArrayList<>())
            .size());
    assertEquals(12, getCollection().countDocuments());
  }

  @Test
  void testSupportedStreamingWriteAppend() throws TimeoutException {
    SparkSession spark = getOrCreateSparkSession();

    StructType schema =
        createStructType(
            asList(
                createStructField("age", DataTypes.LongType, true),
                createStructField("name", DataTypes.StringType, true)));

    Dataset<Row> df = spark.readStream().schema(schema).json(WRITE_RESOURCES_JSON_PATH);

    StreamingQuery query = df.writeStream().outputMode("Append").format("mongodb").start();
    query.processAllAvailable();
    query.stop();

    assertEquals(10, getCollection().countDocuments());

    assertCollection();
  }

  @Test
  void testSupportedWriteModesWithOptions() {
    SparkSession spark = getOrCreateSparkSession();
    Dataset<Row> df = spark.read().json(WRITE_RESOURCES_JSON_PATH);

    DataFrameWriter<Row> dfw =
        df.write()
            .format("mongodb")
            .mode("Overwrite")
            .option(MongoConfig.COLLECTION_NAME_CONFIG, "coll2");

    dfw.save();
    assertEquals(0, getCollection().countDocuments());
    assertEquals(10, getCollection("coll2").countDocuments());

    dfw.option(MongoConfig.COLLECTION_NAME_CONFIG, "coll3").save();
    assertEquals(10, getCollection("coll3").countDocuments());
    assertCollection("coll3");
  }

  @Test
  void testUnsupportedDatasourceV2WriteModes() {
    SparkSession spark = getOrCreateSparkSession();
    DataFrameWriter<Row> dfw =
        spark.read().json(WRITE_RESOURCES_JSON_PATH).write().format("mongodb");

    assertThrows(AnalysisException.class, dfw::save); // Error if exists is the default
    assertThrows(AnalysisException.class, () -> dfw.mode("ErrorIfExists").save());
    assertThrows(AnalysisException.class, () -> dfw.mode("Ignore").save());
  }

  private void assertCollection() {
    assertCollection(getCollectionName());
  }

  private void assertCollection(final String collectionName) {
    List<BsonDocument> expected =
        getOrCreateSparkSession()
            .read()
            .json(WRITE_RESOURCES_JSON_PATH)
            .toJSON()
            .collectAsList()
            .stream()
            .map(BsonDocument::parse)
            .peek(d -> d.put("age", d.getOrDefault("age", BsonNull.VALUE)))
            .collect(Collectors.toList());

    ArrayList<BsonDocument> actual =
        getCollection(collectionName)
            .find()
            .projection(Projections.excludeId())
            .map(
                d ->
                    BsonDocument.parse(
                        d.toJson())) // Parse as simple json for simplified numeric values
            .into(new ArrayList<>());
    assertIterableEquals(expected, actual);
  }
}
