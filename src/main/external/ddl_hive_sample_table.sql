DROP TABLE IF EXISTS <NOME_DATABASE>.<NOME_TABELLA>;
CREATE EXTERNAL TABLE IF NOT EXISTS <NOME_DATABASE>.<NOME_TABELLA> (
	<NOME_COLONNA_1> <TIPO_COLONNA_1>,
	...
	<NOME_COLONNA_N> <TIPO_COLONNA_N>
)
STORED AS ORC
LOCATION '<PATH_HDFS>'
TBLPROPERTIES("orc.compress"="<COMPRESSION>");