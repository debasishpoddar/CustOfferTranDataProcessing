package com.spk2hv.poc.processor


import com.spk2hv.poc.records.OfferData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame,Row }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._
import com.spk2hv.poc.infra.Infrautility

class OfferDataProcessor {
  
  val appName ="Offer Data Processor"  
  val infraUtility = new Infrautility
  
  def processOfferData(dataLocation:String) :Dataset[OfferData]= {
    
    val sparkSession = infraUtility.getHiveEnabledSparkSession(appName)
    val offerData = infraUtility.readTextFile(sparkSession, dataLocation)
    
    import sparkSession.implicits._
    
    val offerProcessedData = offerData
                   .map{case Row(s: String) => s.split(",")}
                   .map(x => OfferData(x(0).toInt, x(1).toInt,x(2).toString(),x(3).toString(),x(4).toString()))
    
    offerProcessedData 
    
  }
  
}