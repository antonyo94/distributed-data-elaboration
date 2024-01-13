#Impostare la directory home del progetto
PATH_HOME="/home/aabate/sparkdataelaboration"

#Verifica argomenti in input 
if [ -z "$1" ];
  then
    echo "ERRORE: nessun argomento inserire in input. Inserire il task da completare"
else
#Variabili di configurazione
LIBS_PATH="$PATH_HOME/target"
LOGS_PATH="$PATH_HOME/logs"
CONFIG_FILE_HDFS_PATH=<PATH HDFS FILE CONFIG>
DATE=$(date '+%Y-%m-%d')

#Proprietà configurazione Spark
DRIVER_CORES=2
DRIVER_MEM=4G
EXECUTOR_CORES=3
EXECUTOR_MEM=4G
EXECUTOR_NUM=3
DEFAULT_PARALLELISM=9
SQL_PARALLELISM=9

#Configurazione ed esecuzione del Job
/usr/hdp/current/spark2-client/bin/spark-submit \
--master yarn \
--driver-class-path /usr/share/java/mssql-jdbc.jar \
--class it.aabate.spark.App \
 --deploy-mode client \
 --num-executors $EXECUTOR_NUM \
 --executor-cores $EXECUTOR_CORES \
 --executor-memory $EXECUTOR_MEM \
 --driver-memory $DRIVER_MEM \
 --conf spark.default.parallelism=$DEFAULT_PARALLELISM \
 --conf spark.sql.shuffle.partitions=$SQL_PARALLELISM \
 --conf spark.hadoop.hive.exec.orc.default.block.size=134217728 \
 --conf "spark.network.timeout=240s" \
 --conf "spark.executor.heartbeatInterval=20s" \
 --conf "spark.sql.warehouse.dir=/user/ponlegalita/spark-warehouse" \
 --conf "spark.driver.maxResultSize=2g" \
$LIBS_PATH/<NOME_FILE_ESEGUIBILE>-1.0-SNAPSHOT-jar-with-dependencies.jar \
$CONFIG_FILE_HDFS_PATH > $LOGS_PATH/log_<NOME_FILE_DI_LOG>_${DATE}.log 2>&1
fi