package it.aabate.spark

import org.apache.spark.sql.{DataFrame, Column, SparkSession}
import org.apache.spark.sql.functions.{col, when, lit, max}


object Writer {

    /** Elaborazione dei dati presenti su HDFS a partire da una query definita in input.
    *   Il risultato dell'elaborazione viene memorizzato nel path HDFS di destinazione.
    *
    *   @param nome_tabella 
    *   @param path_destinazione_l2_all
    *   @param queryHive
    *   @param write_mode
    *   @return (stato_elaborazione_l2, numero_record_l2)
    */
    def insert(nome_tabella:String, path_destinazione_l2_all:String, queryHive: String, write_mode: String) : (String, String) = {
        var stato_elaborazione_l2 = ""
        var numero_record_l2 = ""
        try{
            val spark = SparkBuilder.getSparkSession()
            println(s"****INFO****: Inizio elaborazione tabella ${nome_tabella.toUpperCase}")
            println(s"****INFO****: di seguito viene riportata la query Hive")
            println(s"****INFO****: ${queryHive}")
            val df = spark.sql(queryHive)
            numero_record_l2 = df.count().toString
            df.write.mode(write_mode).format("orc").option("compression","snappy").save(path_destinazione_l2_all)
            stato_elaborazione_l2 = "S"
            println(s"****INFO****: Scrittura della tabella ${nome_tabella.toUpperCase} in modalità ${write_mode} completata su HDFS")
            (stato_elaborazione_l2, numero_record_l2)
        }catch{
            case e: Exception =>{
                println(s"****INFO****: Errore scrittura su HDFS della tabella ${nome_tabella.toUpperCase()} ")
                println(s"****ERRORE****(ERRORE): ${e}")
                numero_record_l2 = 0.toString
                stato_elaborazione_l2 = "E"
                (stato_elaborazione_l2, numero_record_l2)
            }
        }
    }


    /** Conversione dei file CSV in file ORC e scrittura verso la directory di destinazione 
    *
    *   @param nome_tabella
    *   @param queryHive
    *   @param path_sorgente_l1
    *   @param nome_flusso 
    *   @param path_destinazione_l2_all
    *   @param write_mode
    *   @return (stato_elaborazione_l2, numero_record_l2)
    */
    def convert_csv_to_orc(nome_tabella: String, queryHive: String, path_sorgente_l1: String, nome_flusso: String,path_destinazione_l2_all: String, write_mode: String) : (String, String) = {
        var stato_elaborazione_l2 = ""
        var numero_record_l2 = ""
        val regex = ".{3}$".r
        val path_sorgente_l1_json = regex.replaceAllIn(path_sorgente_l1, "JSON")
        val path_sorgente_l1_csv = path_sorgente_l1 + "/" + nome_flusso

        try{
            val spark = SparkBuilder.getSparkSession()          
            import spark.implicits._
            val json = spark.read.format("CSV").option("header","true").option("sep","\u001F").load(path_sorgente_l1_json)
            val listMapping = json.select(col("name"), col("type")).map(r => r.getString(0)+" "+r.getString(1)).collect.toList
            val schema = listMapping.mkString(",")
            println(s"****INFO****: Inizio elaborazione del file CSV ${nome_flusso} della tabella ${nome_tabella.toUpperCase}")           
            println(s"****INFO****: di seguito viene riportata la query Hive")
            println(s"****INFO****: ${queryHive}")
            println(s"****INFO****: di seguito viene riportato lo schema")
            println(s"****INFO****: ${schema}")              
            val csv = spark.read.format("CSV").option("header","true").schema(schema).option("sep","\u001F").option("timestampFormat","yyyyMMddHHmmss").load(path_sorgente_l1_csv)            
            csv.createOrReplaceTempView(nome_tabella)
            val df = spark.sql(queryHive)
            numero_record_l2 = df.count().toString
            df.write.mode(write_mode).format("orc").option("compression","snappy").save(path_destinazione_l2_all)
            stato_elaborazione_l2 = "S"
            println(s"***INFO****: Scrittura dati del file ${nome_flusso} della tabella ${nome_tabella.toUpperCase} in modalità ${write_mode} completata su HDFS")
            (stato_elaborazione_l2, numero_record_l2)
        }catch{
            case e: Exception =>{
                println(s"****INFO****: Errore scrittura su HDFS della tabella ${nome_tabella.toUpperCase()} ")
                println(s"****ERRORE****(ERRORE): ${e}")
                numero_record_l2 = 0.toString
                stato_elaborazione_l2 = "E"
                (stato_elaborazione_l2, numero_record_l2)
            }
        }
    }
    
}
