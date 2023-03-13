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
package com.mongodb.spark.sql.connector.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;

class MongoSparkConnectorReadTest extends MongoSparkConnectorTestCase {

  @Test
  void testReadData() {
    // DorisInput_1271888_17_28 \ PythonExecutor_df1_42_4399_20242
    SparkSession spark =
        SparkSession.builder().master("local").appName("Simple Application").getOrCreate();

    Dataset<Row> dataset =
        spark
            .read()
            .format("mongodb")
            .option(
                "connection.uri",
                "mongodb://192.168.11.138:27017,192.168.11.139:27017,192.168.11.140:27017/ai_model_dev?slaveOk=true")
            .option("database", "ai_model_dev")
            .option("collection", "PythonExecutor_df1_42_4399_20242")
            .load()
            .limit(2);
    dataset.show();
  }
}
