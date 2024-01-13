package it.aabate.spark

import java.sql.Connection

object DelaborazioniManager {
  
  /** Scrittura record sulla tabella di monitoraggio
  *   @param Connection 
  *   @param nome_flusso
  *   @param fonte
  *   @param nome_tabella
  *   @param data_riferimento
  *   @param data_inizio_elaborazione_l2
  *   @param log_file_l2
  *   @param path_destinazione_l2_all
  *   @param nome_file_l2_all
  *   @param stato_elaborazione_l2
  */
  def insert(conn: Connection, nome_flusso: String, fonte: String, nome_tabella: String,data_riferimento:String, data_inizio_elaborazione_l2:String,log_file_l2:String,path_destinazione_l2_all:String,nome_file_l2_all:String, stato_elaborazione_l2:String): Unit ={
    try{
      val insertStmt = conn.createStatement
      val insertQuery = s"INSERT INTO D_ELABORAZIONI (NOME_FLUSSO,DATA_RIFERIMENTO,FONTE,NOME_TABELLA,DATA_INIZIO_ELABORAZIONE_L2,STATO_ELABORAZIONE_L2,LOG_FILE_L2,PATH_DESTINAZIONE_L2_ALL,NOME_FILE_L2_ALL) VALUES ('${nome_flusso}', '${data_riferimento}', '${fonte}', '${nome_tabella}', '${data_inizio_elaborazione_l2}','${stato_elaborazione_l2}','${log_file_l2}','${path_destinazione_l2_all}','${nome_file_l2_all}')"
      insertStmt.execute(insertQuery)
    }catch {
      case e: Exception => {
        println(s"****INFO****: Errore inserimento D_ELABORAZIONI")
        println(s"****ERRORE****: ${e}")
      }
    }
  }


  /** Aggiornamento delle informazioni sulla tabella di monitoraggio relative all'elaborazione su L2.
  *   Questa funzione torna utile nel momento in cui si esegue l'elaborazione dei file CSV provenienti da L1
  *   @param Connection 
  *   @param nome_flusso
  *   @param data_riferimento
  *   @param data_inizio_elaborazione_l2
  *   @param log_file_l2
  *   @param path_destinazione_l2_all
  *   @param nome_file_l2_all
  *   @param stato_elaborazione_l2
  */
  def insert_l2_info(conn: Connection, nome_flusso: String, data_riferimento: String, data_inizio_elaborazione_l2: String, log_file_l2: String, path_destinazione_l2_all: String, nome_file_l2_all: String, stato_elaborazione_l2: String): Unit = {
      try{
          val insertL2InfoStmt = conn.createStatement
          val insertL2InfoQuery = s"UPDATE D_ELABORAZIONI SET DATA_INIZIO_ELABORAZIONE_L2='${data_inizio_elaborazione_l2}', STATO_ELABORAZIONE_L2='${stato_elaborazione_l2}', LOG_FILE_L2='${log_file_l2}', PATH_DESTINAZIONE_L2_ALL='${path_destinazione_l2_all}', NOME_FILE_L2_ALL='${nome_file_l2_all}' WHERE NOME_FLUSSO='${nome_flusso}' AND DATA_RIFERIMENTO='${data_riferimento}'"
          insertL2InfoStmt.execute(insertL2InfoQuery)
      }catch{
        case e: Exception => {
          println(s"****INFO****: Errore inserimento D_ELABORAZIONI")
          println(s"****ERRORE****: ${e}")
        }
      }
  }

  /** Aggiornamento record sulla tabella di monitoraggio
  *   @param Connection 
  *   @param nome_flusso
  *   @param nome_tabella
  *   @param data_riferimento
  *   @param stato_elaborazione_l2
  *   @param numero_record_l2
  *   @param data_fine_elaborazione_l2
  */
  def update(conn: Connection, nome_flusso: String, nome_tabella: String,data_riferimento:String, stato_elaborazione_l2:String, numero_record_l2: String, data_fine_elaborazione_l2 : String): Unit ={
    try{
      val updatetStmt = conn.createStatement
      val data_fine_elaborazione_l2 = java.time.LocalDateTime.now.toString
      val updateQuery = s"UPDATE D_ELABORAZIONI SET STATO_ELABORAZIONE_L2 = '${stato_elaborazione_l2}', DATA_FINE_ELABORAZIONE_L2='${data_fine_elaborazione_l2}', NUMERO_RECORD_L2='${numero_record_l2}' WHERE NOME_FLUSSO = '${nome_flusso}' AND DATA_RIFERIMENTO='${data_riferimento}' "
      updatetStmt.execute(updateQuery)
    }catch {
      case e: Exception => {
        println(s"****INFO****: Errore aggiornamento D_ELABORAZIONI")
        println(s"****ERRORE****: ${e}")
      }
    }finally {
      println(s"****INFO****: Elaborazione dati tabella ${nome_tabella.toUpperCase} conclusa con esito: ${stato_elaborazione_l2}")
    }
  }


  /** Lettura dei file CSV da elaborare dalla tabella di monitoraggio
  *   @param Connection
  *   @param fonte
  *   @param nome_tabella
  *   @return List(path_destinazione_l1, nome_flusso, data_riferimento)
  */
  def get_csv_to_elaborate(conn: Connection, fonte: String, nome_tabella: String): List[(String, String, String)] = {
    var listOfCsv : List[(String, String, String)] = List()
    try{ 
      println(s"****INFO****: Recupero i CSV da elaborare per la tabella ${nome_tabella} dalla D_ELABORAZIONI")     
      val getCsvStmt = conn.createStatement
      val getCsvQuery = s"SELECT ISNULL(PATH_DESTINAZIONE_L1,'nd') AS PATH_DESTINAZIONE_L1, NOME_FLUSSO, DATA_RIFERIMENTO FROM D_ELABORAZIONI WHERE NOME_TABELLA = '${nome_tabella}' AND FONTE = '${fonte}' AND ELABORAZIONE_L1 = 'S' AND STATO_ELABORAZIONE_L2 IS NULL ORDER BY DATA_RIFERIMENTO"
      val resultSet = getCsvStmt.executeQuery(getCsvQuery)
      while ( resultSet.next() ) {
        val path_destinazione_l1 = resultSet.getString("PATH_DESTINAZIONE_L1")
        val nome_flusso = resultSet.getString("NOME_FLUSSO")
        val data_riferimento = resultSet.getString("DATA_RIFERIMENTO")
        println(s"****INFO****: Il seguente file CSV sarà elaborato: ${path_destinazione_l1}/${nome_flusso}")
        listOfCsv = listOfCsv :+ (path_destinazione_l1, nome_flusso, data_riferimento)
      }
      listOfCsv
    }catch {
      case e: Exception => {
        println(s"****INFO****: Errore recupero file da elaborare dalla D_ELABORAZIONI")
        println(s"****ERRORE****(ERRORE): ${e}")
        listOfCsv
      }
    }
  }

}
