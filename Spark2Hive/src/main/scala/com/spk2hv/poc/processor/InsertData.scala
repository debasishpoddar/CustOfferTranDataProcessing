package com.spk2hv.poc.processor

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame,Row }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._
import org.apache.spark.sql.Encoder
import java.text.SimpleDateFormat

import java.sql.Date
import java.sql.Timestamp

import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import com.spk2hv.poc.infra.Infrautility
import com.spk2hv.poc.records.ProcessedData

object InsertData {

  def main(args: Array[String]) {
    
   
    if (args.length < 3 || args.length >3) {
      println("ERROR : Please provide the data  files location in following format (Customer Data,Offer Data,Transaction Data")
      System.exit(1)
    } 
    
    val insertdatahive = new InsertData
    insertdatahive.processStagingData(args(0), args(1), args(2))
    
   /* insertdatahive.processStagingData(Thread.currentThread().getContextClassLoader().getResource("Customer1.csv").toString(), 
                                      Thread.currentThread().getContextClassLoader().getResource("Offer1.csv").toString(), 
                                      Thread.currentThread().getContextClassLoader().getResource("Transaction1.csv").toString()) */
    
    println("Data processed successfully ....")
    
  }
}


class InsertData {
  
  val appName="Processing Staging Data"
  val infraUtility = new Infrautility
  val custdataprocess = new CustomerDataProcessor
  val offdataprocess = new OfferDataProcessor
  val trandataprocess = new TransactionDataProcessor
  
  val table_name = "stagngtbl"
  val partition_column1= "TranYear"
  val partition_column2= "TranWeekOfYear"
  
  
  
  def processStagingData(dataLocation:String,dataLocation1:String,dataLocation2:String) :Dataset[Row]={
    
    val sparkSession = infraUtility.getSparkSession(appName)
    
    import sparkSession.implicits._
    
    val customerdataset = custdataprocess.processCustomerData(dataLocation)
    val offerdataset = offdataprocess.processOfferData(dataLocation1)
    val trandataset = trandataprocess.processOfferData(dataLocation2)
    
    val custoffdataset= customerdataset.join(offerdataset,Seq("Customer_id"), "inner")
                                       .withColumn("Customer_age", (datediff(current_date(),col("Customer_dob"))/365))
                                       .withColumn("Customer_age_group", (when(datediff(current_date(),col("Customer_dob"))/365>=60, "senior"))
                                           .when(datediff(current_date(),col("Customer_dob"))/365<59 && 
                                               datediff(current_date(),col("Customer_dob"))/365>=22, "middleaged")
                                           .when(datediff(current_date(),col("Customer_dob"))/365<22 && 
                                               datediff(current_date(),col("Customer_dob"))/365>=16, "teenager")
                                           .otherwise("child")).withColumn("TranWeekofYear",weekofyear(col("Start_date")))
                                       .withColumn("TranYear", year(col("Start_date")))
                                       .withColumn("TranDayofWeek", date_format(col("Start_date"), "u"))
    
    val custtrandataset= customerdataset.join(trandataset,Seq("Customer_id"), "inner")
                                        .withColumn("Customer_age", (datediff(current_date(),col("Customer_dob"))/365))
                                        .withColumn("Customer_age_group", (when(datediff(current_date(),col("Customer_dob"))/365>=60, "senior"))
                                            .when(datediff(current_date(),col("Customer_dob"))/365<59 && 
                                                datediff(current_date(),col("Customer_dob"))/365>=22, "middleaged")
                                            .when(datediff(current_date(),col("Customer_dob"))/365<22 && 
                                                datediff(current_date(),col("Customer_dob"))/365>=16, "teenager")
                                            .otherwise("child"))
                                        .withColumn("TranWeekofYear",weekofyear(col("Tran_date")))
                                        .withColumn("TranYear", year(col("Tran_date")))
                                        .withColumn("TranDayofWeek", date_format(col("Tran_date"), "u"))
                                        
    val finalcustoffdataset=custoffdataset.groupBy("Customer_id","Customer_age","Customer_age_group","TranYear","TranWeekofYear","TranDayofWeek").agg(count("Start_date").as("no_offers_received")).withColumn("total_sales", lit(0)).withColumn("no_store_visits", lit(0)).withColumn("no_offers_redeemed", lit(0))
    val finalcusttrandataset=custtrandataset.groupBy("Customer_id","Customer_age","Customer_age_group","TranYear","TranWeekofYear","TranDayofWeek").agg(count(when($"Offer_id" !== 0, $"Offer_id")).as("no_offers_redeemed"),count("Transaction_id").as("no_store_visits"),sum("Sales").as("total_sales")).withColumn("no_offers_received", lit(0))
    
    
    val unioncustoffdataset=finalcustoffdataset.selectExpr("Customer_id","cast(Customer_age as int) Customer_age","Customer_age_group","no_offers_received","no_offers_redeemed","no_store_visits","cast(total_sales as Double) total_sales","cast(TranYear as int) TranYear","cast(TranWeekofYear as int) TranWeekofYear","cast(TranDayofWeek as int) TranDayofWeek")
    val unioncusttrandataset=finalcusttrandataset.selectExpr("Customer_id","cast(Customer_age as int) Customer_age","Customer_age_group","no_offers_received","no_offers_redeemed","no_store_visits","cast(total_sales as Double) total_sales","cast(TranYear as int) TranYear","cast(TranWeekofYear as int) TranWeekofYear","cast(TranDayofWeek as int) TranDayofWeek")                                    
    
    val finalstagingdataset=unioncustoffdataset.union(unioncusttrandataset)
    
    finalstagingdataset
  }
  
  def HiveInsertData(dataFile:Dataset[Row]){
  
    val processeddataset=dataFile
    
    try{
    processeddataset.write.partitionBy(partition_column1,partition_column2).saveAsTable(table_name)
        }
    
    catch{
        case ex: Exception => {
        println("Data not Inserted")
        println(ex.printStackTrace().toString)
        ex.printStackTrace()
        }
         }
    
    
  }
  
  
}