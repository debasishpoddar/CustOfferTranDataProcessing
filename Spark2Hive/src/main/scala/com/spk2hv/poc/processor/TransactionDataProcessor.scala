package com.spk2hv.poc.processor

import com.spk2hv.poc.infra.Infrautility
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame,Row }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._
import com.spk2hv.poc.records.TransactionData


class TransactionDataProcessor {
  
  val appName ="Transaction Data Processor"  
  val infraUtility = new Infrautility
  
  def processOfferData(dataLocation:String) :Dataset[TransactionData]= {
    
    val sparkSession = infraUtility.getHiveEnabledSparkSession(appName)
    val transactionData = infraUtility.readTextFile(sparkSession, dataLocation)
    
    import sparkSession.implicits._
    
    val transactionProcessedData = transactionData
                  .map{case Row(s: String) => s.split(",")}
                  .map(x => TransactionData(x(0).toInt, x(1).toInt,x(2).toInt,x(3).toInt,x(4).toInt,x(5).toDouble,x(6).toString))
    
    transactionProcessedData
  
  
}
  
}