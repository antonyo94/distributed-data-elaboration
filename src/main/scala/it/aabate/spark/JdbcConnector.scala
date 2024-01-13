package it.aabate.spark

import java.sql.DriverManager
import java.sql.Connection
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory

object JdbcConnector {
  
  /** Configurazione connessione JDBC
  *   @param jdbcRDMS
  *   @param driverClass
  *   @param jdbcHostname
  *   @param jdbcUsername
  *   @param jdbcPort
  *   @param jdbcDatabase
  *   @param jdbcPassword
  *   @return connection
  */
  def getConnection(jdbcRDMS:String, driverClass:String, jdbcHostname:String, jdbcUsername:String, jdbcPort:String, jdbcDatabase:String, jdbcPassword:String) : Connection = {
        val jdbcUrl = s"jdbc:${jdbcRDMS}://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};user=${jdbcUsername};password=${jdbcPassword};"
        DriverManager.getConnection(jdbcUrl)
        
  }
}
