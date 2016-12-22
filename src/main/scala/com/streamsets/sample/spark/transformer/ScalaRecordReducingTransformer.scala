package com.streamsets.sample.spark.transformer

import com.streamsets.pipeline.api.{Field, Record}
import com.streamsets.pipeline.spark.api.{SparkTransformer, TransformResult}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.PairFunction

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class ScalaRecordReducingTransformer extends SparkTransformer with Serializable {
  val KEY_FIELD_NAME = "key"
  val AGGREGATED_LIST_FIELD_NAME = "aggregatedListField"
  val KEY_FIELD_PATH = "/" + KEY_FIELD_NAME
  val AGGREGATED_LIST_FIELD_PATH = "/" + AGGREGATED_LIST_FIELD_NAME

  //Check whether the record already has /aggregatedListField
  //If so add all the list fields in /aggregateListField to the result
  //if not add the records root field to the result
  def getAggregatedFieldsForRecordFn: (Record) => List[Field] = (r)  => {
    var resultListBuffer = ListBuffer[Field]()
    val aggregatedField = r.get(AGGREGATED_LIST_FIELD_PATH)
    if (aggregatedField != null) {
      r.delete(AGGREGATED_LIST_FIELD_PATH)
      resultListBuffer ++= aggregatedField.getValueAsList
    } else {
      resultListBuffer += Field.create(r.get.getValueAsMap)
    }
    resultListBuffer.toList
  }

  override def transform(recordRDD: JavaRDD[Record]): TransformResult = {
    //Filter error records, record with no '/key' or records with '/key', but the field type is not long/integer/short
    val errorRdd = recordRDD.rdd.filter( r => {
      !r.has(KEY_FIELD_PATH) || !r.get(KEY_FIELD_PATH).getType.isOneOf(Field.Type.LONG, Field.Type.INTEGER, Field.Type.SHORT)
    })

    //Filter non error records, map to long key, value based on '/key' field
    //then run a reducer by key to reduce all the records with the same key to one record
    //And get all the records from the (key, record)
    val resultRdd = recordRDD.rdd.filter(r => {
      r.has(KEY_FIELD_PATH) && r.get(KEY_FIELD_PATH).getType.isOneOf(Field.Type.LONG, Field.Type.INTEGER, Field.Type.SHORT)
    }).map(r => {
      val key = r.get(KEY_FIELD_PATH).getValueAsLong
      r.delete(KEY_FIELD_PATH)
      (key, r)
    }).reduceByKey((r1, r2) => {
      //Merge Lists
      val aggregatedFieldListResult = getAggregatedFieldsForRecordFn(r1) ::: getAggregatedFieldsForRecordFn(r2)
      //Set root field again with all aggregateFields
      val rootMap = Map(AGGREGATED_LIST_FIELD_NAME -> Field.create(aggregatedFieldListResult))
      r1.set(Field.create(rootMap))
      r1
    }).map(rdd => {
      val record = rdd._2
      val key = rdd._1
      record.get.getValueAsMap += (KEY_FIELD_NAME -> Field.create(Field.Type.LONG, key))
      record
    })

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
