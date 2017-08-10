/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.relayr.pubsub;

import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options that can be used to configure BigQuery table.  Example borrowed from the example common package
 * ExampleBigQueryTableOptions
 * This example defaults to relayr / sensor_data.
 */
public interface AggregateTableOptions extends GcpOptions {
  @Description("BigQuery dataset name")
  @Default.String("relayr")
  String getAggDataset();
  void setAggDataset(String dataset);

  @Description("BigQuery table name")
  @Default.String("agg_sensor_data")
  String getAggTable();
  void setAggTable(String table);

  @Description("BigQuery table schema")
  TableSchema getAggSchema();
  void setAggSchema(TableSchema schema);

  
}
