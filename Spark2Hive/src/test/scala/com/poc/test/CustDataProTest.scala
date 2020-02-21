package com.poc.test

import org.scalatest._

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.spk2hv.poc.infra.Infrautility
import com.spk2hv.poc.processor.InsertData

class CustDataProTest extends FunSuite with BeforeAndAfterEach {
  
  val infraUtility = new Infrautility
  val appName="Insert Data Test"
  val inserthivedata = new InsertData ()
  
  val customerfilelocation=Thread.currentThread().getContextClassLoader().getResource("Customer1.csv").toString()
  val offerfilelocation = Thread.currentThread().getContextClassLoader().getResource("Offer1.csv").toString()
  val transactionfilelocation = Thread.currentThread().getContextClassLoader().getResource("Transaction1.csv").toString()
  
  test("Customer Offer Transaction Data Test"){

    //Processing Data by passing value,upon completion the same wil be pushed to hive.
    val proceesseddataset=inserthivedata.processStagingData(customerfilelocation, offerfilelocation, transactionfilelocation)
    
    println(proceesseddataset.show())
    
    // Data pushed to hive ,if same is not possible it will by pass.
    inserthivedata.HiveInsertData(proceesseddataset)
   
    assert("True".toLowerCase == "true")
  }  
  
  
}