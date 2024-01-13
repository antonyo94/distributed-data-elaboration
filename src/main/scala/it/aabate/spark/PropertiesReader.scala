package it.aabate.spark

import scala.io.Source
import java.net.URL
import java.util.Properties

object PropertiesReader {
  
    /** Lettura delle properties necessarie alla corretta esecuzione del job dal file config.properties
    *   @param pathConfigProperties 
    *   @return properties
    */
    def getProperties(pathConfigProperties: String) : Properties ={
      
      var properties : Properties = null
      val urlConfigProperties = pathConfigProperties
      val sourceConfigProperties = Source.fromURL(urlConfigProperties)
      properties = new Properties()
      properties.load(sourceConfigProperties.bufferedReader())
      properties

    }
}
