/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.spark.sample.evaluator;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.spark.api.SparkTransformer;
import com.streamsets.pipeline.spark.api.TransformResult;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class RecordWithKeySparkAggregator extends SparkTransformer {
  private static final String KEY = "/key";
  private static final String AGGREGATE_FIELD = "/aggregateField";

  private Record reduceRecord(Record v1, Record v2) {
    Field aggregateField1 = v1.get(AGGREGATE_FIELD);
    Field aggregateField2 = v2.get(AGGREGATE_FIELD);
    List<Field> aggregateField = new ArrayList<>();
    if (aggregateField1 != null) {
      v1.delete(AGGREGATE_FIELD);
      aggregateField.addAll(aggregateField1.getValueAsList());
    } else {
      aggregateField.add(v1.get());
    }
    if (aggregateField2 != null) {
      v2.delete(AGGREGATE_FIELD);
      aggregateField.addAll(aggregateField2.getValueAsList());
    } else {
      aggregateField.add(v2.get());
    }
    LinkedHashMap<String, Field> rootMap = new LinkedHashMap<>();
    rootMap.put(AGGREGATE_FIELD, Field.create(Field.Type.LIST, aggregateField));
    v1.set(Field.createListMap(rootMap));
    return v1;
  }

  @Override
  public TransformResult transform(JavaRDD<Record> recordRDD) {
    //Error record - either record does not have /key or the field /key is not integer/long/short
    JavaPairRDD<Record, String> errorRDD = recordRDD.filter(new Function<Record, Boolean>() {
      @Override
      public Boolean call(Record v1) throws Exception {
        return (!v1.has(KEY) || !v1.get(KEY).getType().isOneOf(Field.Type.INTEGER, Field.Type.SHORT, Field.Type.LONG));
      }
    }).mapToPair(new PairFunction<Record, Record, String>() {
      @Override
      public Tuple2<Record, String> call(Record record) throws Exception {
        return new Tuple2<>(record, "");
      }
    });

    //Now get record with field /key which is integer, short or long
    //and convert it to Pair RDD where key is the field key
    JavaPairRDD<Long, Record> nonErrorRdd = recordRDD.filter(new Function<Record, Boolean>() {
      @Override
      public Boolean call(Record v1) throws Exception {
        return v1.has(KEY) && v1.get(KEY).getType().isOneOf(Field.Type.INTEGER, Field.Type.SHORT, Field.Type.LONG);
      }
    }).mapToPair(new PairFunction<Record, Long, Record>() {
      @Override
      public Tuple2<Long, Record> call(Record record) throws Exception {
        return new Tuple2<>(record.get(KEY).getValueAsLong(), record);
      }
    });

    //Now all these non error rdds are all reduced by key with same value for field /key
    //Where the reduction of two records is just all records with the same key are
    //put in a list field.
    //and map to resulting RDD with just those records
    JavaRDD<Record> resultRDD = nonErrorRdd.reduceByKey(new Function2<Record, Record, Record>() {
      @Override
      public Record call(Record v1, Record v2) throws Exception {
        return reduceRecord(v1, v2);
      }
    }).map(new Function<Tuple2<Long, Record>, Record>() {
      @Override
      public Record call(Tuple2<Long, Record> v1) throws Exception {
        return v1._2();
      }
    });

    //Construct the resultingRDD and Error RDD
    return new TransformResult(resultRDD, errorRDD);
  }
}
