package it.aabate.spark
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

object SparkBuilder {

    /** Configurazione SparkSession
    *   @return spark
    */
  def getSparkSession() : SparkSession = {
    
    SparkSession.builder.appName("SPARK DATA ELABORATION").enableHiveSupport().master("yarn").getOrCreate()   
     
  }
}
