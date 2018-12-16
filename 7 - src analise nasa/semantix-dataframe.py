from pyspark.sql import Row
import re

# Método que utiliza Expressão Regular
# Objetivo retornar as colunas em condição parta análise
# @Param l linha string
# @Return array[]
def getLinhaArray(l):
	ret = []
	groups = re.match( r'^(.*?) (.*?) (.*?) \[(.*)\] \"(.*)\" (\d{3}) (.*)', l, re.M|re.I)
	for i in range(1, 8):
		ret.append(groups.group(i))
	return ret

	
# Instancia do Spark Context
sc = spark.sparkContext

# Carregando os arquivos Jul e Aug de 95 do HDFS
rddJuleAug95 = sc.textFile("access_log_*")
parts = rddJuleAug95.map(lambda linha: getLinhaArray(linha))
logs = parts.map(lambda p: Row(host=p[0],data=p[3],requisicao=p[4],codrethttp=p[5],totalbytes=p[6]))

# Criando tabela Temporária para Consulta
schemaLogs = spark.createDataFrame(logs)
schemaLogs.createOrReplaceTempView("logs")

# Quantidades de Registros no Dataset
spark.sql("SELECT count(*) as qtd FROM logs").show()
# +-------+
# |    qtd|
# +-------+
# |3461612|
# +-------+


# Número de hosts únicos
hostsunicos = spark.sql("SELECT count(distinct host) as nrohosts FROM logs").collect()
print('Numero de Hosts Unicos: ' + str(hostsunicos[0].nrohosts)) # Numero de Hosts Unicos: 137978

# O total de erros 404.
nroPags404 = spark.sql("SELECT count(codrethttp) as nroPags404 FROM logs WHERE codrethttp = '404'").collect()
print('\nTotal de erros 404: ' + str(nroPags404[0].nroPags404)) # Total de erros 404: 20901

# Os 5 URLs que mais causaram erro 404.
urlMaisPags404 = spark.sql("SELECT concat(host,requisicao) as url, count(codrethttp) as nroPags404 FROM logs WHERE codrethttp = '404' GROUP BY url ORDER BY nroPags404 DESC LIMIT 5").collect()
print('\nTotal de erros 404: ')
i = 1
for lUrlMaisPags404 in urlMaisPags404:
	print('\nTop ' + str(i) +': '+ (lUrlMaisPags404.url).replace('GET ','').replace(' HTTP/1.0',''))
	i = i + 1

# Total de erros 404:
# Top 1: ts8-1.westwood.ts.ucla.edu/images/Nasa-logo.gif
# Top 2: nexus.mlckew.edu.au/images/nasa-logo.gif
# Top 3: 203.13.168.17/images/nasa-logo.gif
# Top 4: 203.13.168.24/images/nasa-logo.gif
# Top 5: crl5.crl.com/images/nasa-logo.gif

# Quantidade de erros 404 por dia.
nroPags404Diario = spark.sql("SELECT cast(from_unixtime(unix_timestamp(data,'dd/MMM/yyyy:HH:mm:ss Z')) as date) as dt, count(codrethttp) as nroPags404 FROM logs where codrethttp = '404' group by dt ORDER BY dt").collect()
print('\nLista de Erros Diarios: ')
for lnroPags404Diario in nroPags404Diario:
	print('\nDia ' + str(lnroPags404Diario.dt) +' => '+ str(lnroPags404Diario.nroPags404))

# Lista de Erros Diarios:
# Dia 1995-07-01 => 251
# Dia 1995-07-02 => 290
# Dia 1995-07-03 => 449
# Dia 1995-07-04 => 388
# Dia 1995-07-05 => 454
# Dia 1995-07-06 => 605
# Dia 1995-07-07 => 616
# Dia 1995-07-08 => 349
# Dia 1995-07-09 => 306
# Dia 1995-07-10 => 402
# Dia 1995-07-11 => 505
# Dia 1995-07-12 => 445
# Dia 1995-07-13 => 541
# Dia 1995-07-14 => 421
# Dia 1995-07-15 => 261
# Dia 1995-07-16 => 224
# Dia 1995-07-17 => 422
# Dia 1995-07-18 => 489
# Dia 1995-07-19 => 625
# Dia 1995-07-20 => 443
# Dia 1995-07-21 => 344
# Dia 1995-07-22 => 200
# Dia 1995-07-23 => 191
# Dia 1995-07-24 => 342
# Dia 1995-07-25 => 449
# Dia 1995-07-26 => 373
# Dia 1995-07-27 => 329
# Dia 1995-07-28 => 131
# Dia 1995-08-01 => 243
# Dia 1995-08-03 => 222
# Dia 1995-08-04 => 373
# Dia 1995-08-05 => 240
# Dia 1995-08-06 => 393
# Dia 1995-08-07 => 519
# Dia 1995-08-08 => 356
# Dia 1995-08-09 => 321
# Dia 1995-08-10 => 295
# Dia 1995-08-11 => 298
# Dia 1995-08-12 => 171
# Dia 1995-08-13 => 210
# Dia 1995-08-14 => 307
# Dia 1995-08-15 => 323
# Dia 1995-08-16 => 256
# Dia 1995-08-17 => 264
# Dia 1995-08-18 => 261
# Dia 1995-08-19 => 225
# Dia 1995-08-20 => 293
# Dia 1995-08-21 => 308
# Dia 1995-08-22 => 256
# Dia 1995-08-23 => 375
# Dia 1995-08-24 => 367
# Dia 1995-08-25 => 448
# Dia 1995-08-26 => 357
# Dia 1995-08-27 => 355
# Dia 1995-08-28 => 440
# Dia 1995-08-29 => 420
# Dia 1995-08-30 => 496
# Dia 1995-08-31 => 573
# Dia 1995-09-01 => 91

# O total de bytes retornados.
qtdBytesRetorn = spark.sql("SELECT sum(totalbytes) as qtdBytes FROM logs").collect()
print('\nBytes Retornados: ' + str(qtdBytesRetorn[0].qtdBytes)) # Bytes Retornados: 65524314915.0

exit()