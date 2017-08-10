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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import  org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.examples.common.*;

import java.util.List;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import com.google.gson.*;
import org.joda.time.Duration;



/**
 * A streaming Example using PubSub and BigQuery output.
 *
 * <p>This pipeline example reads data from PubSub and outputs the results
 * a BigQuery table.
 *
 * <p>This example has been modified to override the default BigQuery table from the example common package.
 * It will create a dataset with the name relayr and tablename sensor_data
 * You can override them by using the {@literal --bigQueryDataset}, and {@literal --bigQueryTable}
 * options. If the BigQuery table does not exist, the example will try to create them.
 *
 * <p>The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.  This will not work when running in Direct mode.
 */
public class PubSub {
    static class ExtractDataFn extends DoFn<String, Telemetry> {
    @ProcessElement
    public void processElement(ProcessContext c) {  
    //String s = "{\"reading\": [{\"meaning\":\"pipeTemperature\",\"value\":66,\"received\":1496966201571}]}";
    try {
          
      String s1 = c.element();
      String s2 = s1.replaceAll("\\\\", "");
      String s = s2.substring(1, s2.length()-1);
      JsonArray ja = new JsonParser().parse(s).getAsJsonArray();
      JsonElement obj = ja.get(0);
      Telemetry t = new Telemetry(obj.getAsJsonObject().get("meaning").getAsString(), obj.getAsJsonObject().get("value").getAsInt(), obj.getAsJsonObject().get("received").getAsLong());
      c.output(t);
     }catch (Exception e) {
       System.out.println("Error: "  + e.toString());
       return;
     }
    }
  }
    

@DefaultCoder(AvroCoder.class)
static class Telemetry {
    @Nullable String measure;
    @Nullable Integer value;
    @Nullable Long received;


    public Telemetry() {}

    public Telemetry(String measure, Integer value, Long received) {
      this.measure = measure;
      this.value = value;
      this.received = received;
    }

    public String getMeasure() {
      return this.measure;
    }
    public Integer getValue() {
      return this.value;
    }
    public Long getReceived() {
      return this.received;
    }
}


@DefaultCoder(AvroCoder.class)
static class AggTelemetry {
    @Nullable String measure;
    @Nullable Integer max;
    @Nullable Integer min;
    @Nullable Float  avg;
    @Nullable Long    cnt;
    @Nullable String  ts;


    public AggTelemetry() {}

    public AggTelemetry(String measure, Integer max, Integer min, Float avg, Long cnt, String ts) {
      this.measure = measure;
      this.max = max;
      this.min = min;
      this.avg = avg;
      this.cnt = cnt;
      this.ts = ts;
    }

    public String getMeasure() {
      return this.measure;
    }
    public Integer getMax() {
      return this.max;
    }
    public Integer getMin() {
      return this.min;
    }
    public Float getAvg() {
      return this.avg;
    }

    public Long getCnt() {
      return this.cnt;
    }
     public String getTS() {
      return this.ts;
    }
}


/**
 * Extract the measure and value
 * 
 */
static class ExtractMeasureValueFn extends DoFn<Telemetry, KV<String, Integer>> {
@ProcessElement
    public void processElement(ProcessContext c) {
       

        Telemetry t = c.element();
        c.output(KV.of(t.getMeasure(),t.getValue()));
        
    }
}

  /**
   * Converts Telemetry into BigQuery rows.
   */
   static class FormatTelemetryFn extends DoFn<Telemetry, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      Telemetry tel = c.element();
      TableRow row = new TableRow()
          .set("measure", tel.getMeasure())
          .set("value", tel.getValue())
          .set("received", tel.getReceived())
          .set("window_timestamp", c.timestamp().toString());
      c.output(row);
    }

    /**
     * Defines the BigQuery schema used for the output.
     */
    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("measure").setType("STRING"));
      fields.add(new TableFieldSchema().setName("value").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("received").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP"));
      TableSchema schema = new TableSchema().setFields(fields);
      return schema;
    }
}

/**
   * Converts Aggregated data into BigQuery rows.
   */
   static class FormatAggTelemetryFn extends DoFn<AggTelemetry, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      AggTelemetry tel = c.element();
      TableRow row = new TableRow()
          .set("measure", tel.getMeasure())
          .set("max", tel.getMax())
          .set("min", tel.getMin())
          .set("avg", tel.getAvg())
          .set("cnt", tel.getCnt())
          .set("ts", tel.getTS());
      c.output(row);
    }

    /**
     * Defines the BigQuery schema used for the output.
     */
    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("measure").setType("STRING"));
      fields.add(new TableFieldSchema().setName("max").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("min").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("avg").setType("FLOAT"));
      fields.add(new TableFieldSchema().setName("cnt").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("ts").setType("STRING"));
      TableSchema schema = new TableSchema().setFields(fields);
      return schema;
    }
}

  /**
   * Options supported by {@link StreamingDataExtract}.
   * 
   */
  private interface StreamingDataExtractOptions
      extends ExampleOptions, ExampleBigQueryTableOptions, AggregateTableOptions, ExamplePubsubTopicAndSubscriptionOptions, StreamingOptions {
  
  }

 
  
  /**
   * Sets up and starts streaming pipeline.
   *
   * @throws IOException if there is a problem setting up resources
   */
  public static void main(String[] args) throws IOException {

       
    StreamingDataExtractOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(StreamingDataExtractOptions.class);
    options.setStreaming(true);
    options.setBigQuerySchema(FormatTelemetryFn.getSchema());
    options.setAggSchema(FormatAggTelemetryFn.getSchema());
    ExampleUtils exampleUtils = new ExampleUtils(options);
    exampleUtils.setup();

    Pipeline pipeline = Pipeline.create(options);

    /**
     * BigQuery table reference.  References consist of 3 parts.
     * Project, Datastet, and Table.
     */ 
    TableReference tableRef = new TableReference();
    tableRef.setProjectId(options.getProject());
    tableRef.setDatasetId(options.getBigQueryDataset());
    tableRef.setTableId(options.getBigQueryTable());

    TableReference aggTableRef = new TableReference();
    aggTableRef.setProjectId(options.getProject());
    aggTableRef.setDatasetId(options.getAggDataset());
    aggTableRef.setTableId(options.getAggTable());

    PCollection<String> p = pipeline.apply("ReadPubSub", PubsubIO.readStrings().fromSubscription(options.getPubsubSubscription()));

    //Windows boundary set
    PCollection<String> p_windowed = p.apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))));
    
    PCollection<Telemetry> t = p_windowed.apply(ParDo.of(new ExtractDataFn()));
    PCollection<KV<String, Integer>> mv = t.apply(ParDo.of(new ExtractMeasureValueFn()));
    PCollection<KV<String, Integer>> maxPerKey = mv.apply(Max.<String>integersPerKey());
    PCollection<KV<String, Integer>> minPerKey = mv.apply(Min.<String>integersPerKey());
    PCollection<KV<String, Double>> meanPerKey = mv.apply(Mean.<String, Integer>perKey());
    PCollection<KV<String, Long>> cntPerKey = mv.apply(Count.<String, Integer>perKey());

    //Perform a join of the aggregated results. 
    // Create tuple tags for the value types in each collection.
    final TupleTag<Integer> tag1 = new TupleTag<Integer>();
    final TupleTag<Integer> tag2 = new TupleTag<Integer>();
    final TupleTag<Double> tag3 = new TupleTag<Double>();
    final TupleTag<Long> tag4 = new TupleTag<Long>();

    //Windows applied
    PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
    KeyedPCollectionTuple.of(tag1, maxPerKey)
                         .and(tag2, minPerKey)
                         .and(tag3, meanPerKey)
                         .and(tag4, cntPerKey)
                         .apply(CoGroupByKey.<String>create());

 // Create a collection reflecting the aggregrated results. 
  PCollection<AggTelemetry> aggResults = 
  coGbkResultCollection.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, AggTelemetry>() { 
     @ProcessElement
     public void processElement(ProcessContext c) {
     
     AggTelemetry at = new AggTelemetry(c.element().getKey(), c.element().getValue().getOnly(tag1), c.element().getValue().getOnly(tag2), 
     c.element().getValue().getOnly(tag3).floatValue(),  
     c.element().getValue().getOnly(tag4), c.timestamp().toString());

      c.output(at); 
     } 
    })); 
 
    
    //Create a table row collection and output to BigQuery
    PCollection<TableRow> results = t.apply(ParDo.of(new FormatTelemetryFn()));
    results.apply(BigQueryIO.writeTableRows().to(tableRef).withSchema(FormatTelemetryFn.getSchema()));

    PCollection<TableRow> ar = aggResults.apply(ParDo.of(new FormatAggTelemetryFn()));
    ar.apply(BigQueryIO.writeTableRows().to(aggTableRef).withSchema(FormatAggTelemetryFn.getSchema()));
         

    PipelineResult result = pipeline.run();

    // ExampleUtils will try to cancel the pipeline before the program exists.
    exampleUtils.waitToFinish(result);
  }
}
