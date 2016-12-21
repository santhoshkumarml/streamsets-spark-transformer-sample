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
package com.streamsets.spark.sample.evaluator

import com.streamsets.pipeline.api.{Field, Record}
import com.streamsets.pipeline.spark.api.{SparkTransformer, TransformResult}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.PairFunction

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ScalaSparkRecordReducer extends SparkTransformer with Serializable {
  val KEY_FIELD_PATH = "/key"
  val AGGREGATED_LIST_FIELD_PATH = "/aggregatedListField"

  override def transform(recordRDD: JavaRDD[Record]): TransformResult = {
    //Filter error records, record with no '/key' or records with '/key', but the field type is not long/integer/short
    val errorRdd = recordRDD.rdd.filter( r => {
      !r.has(KEY_FIELD_PATH) || r.get(KEY_FIELD_PATH).getType.isOneOf(Field.Type.LONG, Field.Type.INTEGER, Field.Type.SHORT)
    })

    //Check whether the record already has /aggregatedListField
    //If so add all the list fields in /aggregateListField to the result
    //if not add the records root field to the result
    val addRecordsToAggregatedFieldFn: (Record) => List[Field] = (r)  => {
      var resultListBuffer = ListBuffer[Field]()
      val aggregatedField = r.get(AGGREGATED_LIST_FIELD_PATH)
      if (aggregatedField != null) {
        r.delete(AGGREGATED_LIST_FIELD_PATH);
        resultListBuffer ++= aggregatedField.getValueAsList
      } else {
        val rootField = r.get()
        resultListBuffer += Field.create(rootField.getValueAsMap)
      }
      resultListBuffer.toList
    }

    //Filter non error records, map to long key, value based on '/key' field
    //then run a reducer by key to reduce all the records with the same key to one record
    //And get all the records from the (key, record)
    val resultRdd = recordRDD.rdd.filter(r => {
      r.has(KEY_FIELD_PATH) && r.get(KEY_FIELD_PATH).getType.isOneOf(Field.Type.LONG, Field.Type.INTEGER, Field.Type.SHORT)
    }).map( r => {
      val key = r.get(KEY_FIELD_PATH).getValueAsLong
      r.delete(KEY_FIELD_PATH)
      (key, r)
    }).reduceByKey( (r1, r2) => {
      //Merge Lists
      val recordAggreagtedField = addRecordsToAggregatedFieldFn(r1, List[Field]()) ::: addRecordsToAggregatedFieldFn(r2, List[Field]())
      //Set root field again with all aggregateFields
      val rootMap = mutable.LinkedHashMap[String, Field]()
      rootMap.put(AGGREGATED_LIST_FIELD_PATH, Field.create(recordAggreagtedField))
      r1.set(Field.create(Field.Type.LIST_MAP, rootMap))
      r1
    }).values

    //return transformer result
    new TransformResult(
      resultRdd.toJavaRDD,
      errorRdd.toJavaRDD.mapToPair(new PairFunction[Record, Record, String]{
        override def call(t: Record): (Record, String) = {
          (t, "Does not have field '/key' or value of field '/key' is not a valid integer/short/long")
        }
      })
    )
  }
}
