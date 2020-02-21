package com.spk2hv.poc.processor

import com.spk2hv.poc.records.CustomerData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame,Row }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._
import com.spk2hv.poc.infra.Infrautility



class CustomerDataProcessor {
  
  val appName ="Customer Data Processor"  
  val infraUtility = new Infrautility
  
  def processCustomerData(dataLocation:String) :Dataset[CustomerData]= {
    
    val sparkSession = infraUtility.getHiveEnabledSparkSession(appName)
    val customerData = infraUtility.readTextFile(sparkSession, dataLocation)
    
    import sparkSession.implicits._
    
    val customerProcessedData = customerData
                   .map{case Row(s: String) => s.split(",")}
                   .map(x => CustomerData(x(0).toString(), x(1).toInt,x(2).toString(),x(3).toString()))
    
    customerProcessedData 
    
  }
  
}