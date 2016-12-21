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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class JavaRecordReducingTransformer extends SparkTransformer implements Serializable {
  private static final String KEY_FIELD_PATH = "/key";
  private static final String AGGREGATE_LIST_FIELD_PATH = "/aggregateListField";

  //Check whether the record already has /aggregatedListField
  //If so add all the list fields in /aggregateListField to the result
  //if not add the records root field to the result
  private List<Field> getAggregatedFields(Record record) {
    Field aggregateListFieleInRecord = record.get(AGGREGATE_LIST_FIELD_PATH);
    List<Field> aggregateListField = new ArrayList<>();
    if (aggregateListFieleInRecord != null) {
      record.delete(AGGREGATE_LIST_FIELD_PATH);
      aggregateListField.addAll(aggregateListFieleInRecord.getValueAsList());
    } else {
      Field field = record.get();
      aggregateListField.add(Field.create(field.getValueAsMap()));
    }
    return aggregateListField;
  }

  //Reduce multiple records by adding the root field of the record to aggregatedListField
  private Record reduceRecord(Record record1, Record record2) {
    List<Field> aggregateListField = new ArrayList<>(getAggregatedFields(record1));
    aggregateListField.addAll(getAggregatedFields(record2));
    LinkedHashMap<String, Field> rootMap = new LinkedHashMap<>();
    rootMap.put(AGGREGATE_LIST_FIELD_PATH, Field.create(Field.Type.LIST, aggregateListField));
    record1.set(Field.createListMap(rootMap));
    return record1;
  }

  @Override
  public TransformResult transform(JavaRDD<Record> recordRDD) {
    //Error record - either record does not have /key or the field /key is not integer/long/short
    JavaPairRDD<Record, String> errorRDD = recordRDD.filter(new Function<Record, Boolean>() {
      @Override
      public Boolean call(Record record) throws Exception {
        return !record.has(KEY_FIELD_PATH) || !record.get(KEY_FIELD_PATH).getType().isOneOf(Field.Type.INTEGER, Field.Type.SHORT, Field.Type.LONG);
      }
    }).mapToPair(new PairFunction<Record, Record, String>() {
      @Override
      public Tuple2<Record, String> call(Record record) throws Exception {
        return new Tuple2<>(record, "Does not have field '/key' or value of field '/key' is not a valid integer/short/long");
      }
    });

    //Filter non error records, map to long key, value based on '/key' field
    JavaPairRDD<Long, Record> nonErrorRdd = recordRDD.filter(new Function<Record, Boolean>() {
      @Override
      public Boolean call(Record record) throws Exception {
        return record.has(KEY_FIELD_PATH) && record.get(KEY_FIELD_PATH).getType().isOneOf(Field.Type.INTEGER, Field.Type.SHORT, Field.Type.LONG);
      }
    }).mapToPair(new PairFunction<Record, Long, Record>() {
      @Override
      public Tuple2<Long, Record> call(Record record) throws Exception {
        long key = record.get(KEY_FIELD_PATH).getValueAsLong();
        //Just remove the key for now.
        record.delete(KEY_FIELD_PATH);
        return new Tuple2<>(key, record);
      }
    });

    //Run a reducer by key to reduce all the records with the same key to one record
    //And get all the values from the (key, record) pair rdd
    JavaRDD<Record> resultRDD = nonErrorRdd.reduceByKey(new Function2<Record, Record, Record>() {
      @Override
      public Record call(Record record1, Record record2) throws Exception {
        return reduceRecord(record1, record2);
      }
    }).map(new Function<Tuple2<Long, Record>, Record>() {
      @Override
      public Record call(Tuple2<Long, Record> v1) throws Exception {
        Record record = v1._2();
        LinkedHashMap<String, Field> rootField = record.get().getValueAsListMap();
        rootField.put(KEY_FIELD_PATH, Field.create(Field.Type.LONG, v1._1()));
        return record;
      }
    });

    //Return the transformer result
    return new TransformResult(resultRDD, errorRDD);
  }
}
