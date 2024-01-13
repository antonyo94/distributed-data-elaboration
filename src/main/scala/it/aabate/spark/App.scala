package it.aabate.spark

import java.sql.Connection
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import java.net.URL

object App {
  
  def main(args : Array[String]) {

    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
    var data_riferimento = java.time.LocalDate.now.toString.substring(0,10)
    
    if (args.length == 0) {
      println("****ERRORE****: Inserire il path hdfs del file config.properties")
      return
    }
    val pathConfigProperties = args.apply(0)
    println("****INFO****: Path hdfs del file config.properties - " + pathConfigProperties)

    if (pathConfigProperties != null) {

      //Lettura del file di properties
      val properties = PropertiesReader.getProperties(pathConfigProperties)

      //Apertura connessione al SQL Server per la tabella di monitoraggio
      val conn = JdbcConnector.getConnection(properties.getProperty("jdbcRDMS"), properties.getProperty("driverClassSQLServer"), properties.getProperty("jdbcHostnameSQLServer"), properties.getProperty("jdbcUsernameSQLServer"), properties.getProperty("jdbcPortSQLServer"), properties.getProperty("jdbcDatabaseSQLServer"), properties.getProperty("jdbcPasswordSQLServer"))

      val fonte = properties.getProperty("fonte")
      val logFile = properties.getProperty("logFile")
      val pathDst = properties.getProperty("pathDst")
      
      var stato_elaborazione_l2 = "I"
      var numero_record_l2 = ""      

      properties.getProperty("tabelle").split(",").foreach( table =>
        properties.getProperty("query_" + table + "_operation") match {
          case "insert" => {
            stato_elaborazione_l2 = "I"
            DelaborazioniManager.insert(conn, table, fonte, table, data_riferimento, java.time.LocalDateTime.now.toString, logFile + "_" + data_riferimento + ".log", pathDst + table, table, stato_elaborazione_l2)        
            val hiveResult = Writer.insert(table, pathDst + table, properties.getProperty("query_"+ table), properties.getProperty("query_" + table + "_write_mode") )
            stato_elaborazione_l2 = hiveResult._1
            numero_record_l2 = hiveResult._2  
            DelaborazioniManager.update(conn, table, table, data_riferimento, stato_elaborazione_l2, numero_record_l2, java.time.LocalDateTime.now.toString) 
          }
          case "convert_csv_to_orc" => {
            stato_elaborazione_l2 = "I"
            val write_mode = properties.getProperty("query_" + table + "_write_mode") 
            val listOfCsv = DelaborazioniManager.get_csv_to_elaborate(conn, fonte, table)
            listOfCsv.foreach{
              case(path_destinazione_l1, nome_flusso, data_riferimento) => {
                DelaborazioniManager.insert_l2_info(conn, nome_flusso, data_riferimento, java.time.LocalDateTime.now.toString, logFile + "_" + data_riferimento + ".log", pathDst + table, table, stato_elaborazione_l2)
                val csvResult = Writer.convert_csv_to_orc(table, properties.getProperty("query_"+ table), path_destinazione_l1, nome_flusso, pathDst + table, write_mode)
                stato_elaborazione_l2 = csvResult._1
                numero_record_l2 = csvResult._2
                DelaborazioniManager.update(conn, nome_flusso, table, data_riferimento, stato_elaborazione_l2, numero_record_l2, java.time.LocalDateTime.now.toString) 
              }
            }
          }
          case x => {
            println(s"****ERRORE****: operazione ${x} non definita per la tabella " + table)
          }
        }
      ) 
        
      conn.close()

    }

  }

}