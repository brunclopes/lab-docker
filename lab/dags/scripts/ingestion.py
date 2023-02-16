import pyspark
from pyspark import SparkContext 
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').getOrCreate()

# Bibliotecas Pyspark
from pyspark.sql.functions import current_date, col, to_date, hour, year, month, to_timestamp, dayofmonth
from pyspark.sql.types import StringType

from chaves import access_key, secret_key, endpoint

# Função para conexão do spark com o Data lake s3 
def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"http://{endpoint}")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.SSL.enabled", "false")
    
load_config(spark.sparkContext)

#Leitura dos dados
#Foi realizado a leitura de um arquivo csv, onde cada tabela estava em um arquivo diferente.

list = ['dCopo', 'fPedidos', 'dCidade', 'dEntrega', 'dPagamento', 'dAcompanhamento', 'dCobertura']
path = "s3a://datalake-dados/raw"

df_copo = spark.read.option("delimiter", ";").option("header", "true").csv(f'{path}/{list[0]}.csv')

df_pedidos = spark.read.option("delimiter", ";").option("header", "true").csv(f'{path}/{list[1]}.csv')

df_cidade = spark.read.option("delimiter", ";").option("header", "true").csv(f'{path}/{list[2]}.csv')

df_entrega = spark.read.option("delimiter", ";").option("header", "true").csv(f'{path}/{list[3]}.csv')

df_pagamento = spark.read.option("delimiter", ";").option("header", "true").csv(f'{path}/{list[4]}.csv')

df_acompanhamento = spark.read.option("delimiter", ";").option("header", "true").csv(f'{path}/{list[5]}.csv')

df_cobertura = spark.read.option("delimiter", ";").option("header", "true").csv(f'{path}/{list[6]}.csv')

# Tratamento no Dataframe pedidos, convertendo a coluna DataHora para datetime
df_pedidos = df_pedidos.withColumn('DataHora', to_timestamp('DataHora'))

#Join entre os Dataframes
#Após a leitura, foi realizado join entre o Dataframe Pedidos com os demais Dataframes, com o intuito de criar somente um tabelão, retirando os códigos e substituindo pelas informações descritivas.

# Join com o Dataframe Copo, trazendo informações como o volume do copo e valor do copo
cond = [df_pedidos.Copo == df_copo.IdCopo]
pedidos = df_pedidos.join(df_copo, cond, how='left').drop('IdCopo', 'Copo')

# Join com o Dataframe Cidade, trazendo informações de localização do cliente
cond = [pedidos.Cidade == df_cidade.IdCidade]
pedidos = pedidos.join(df_cidade, cond, how='left').drop('IdCidade', 'Cidade')

# Join com o Dataframe Entrega, trazendo informações sobre o tipo de entrega
cond = [pedidos.Entrega == df_entrega.IdEntrega]
pedidos = pedidos.join(df_entrega, cond, how='left').drop('IdEntrega', 'Entrega')

# Join com o Dataframe Pagamento, trazendo as informações de pagamento do cliente
cond = [pedidos.Pagamento == df_pagamento.IdPagamento]
pedidos = pedidos.join(df_pagamento, cond, how='left').drop('IdPagamento', 'Pagamento')

# Join com o Dataframe Cobertura, trazendo informação da cobertura escolhida pelo cliente
cond = [pedidos.Cobertura == df_cobertura.IdCobertura]
pedidos = pedidos.join(df_cobertura, cond, how='left').drop('IdCobertura', 'Cobertura')


# Join com o Dataframe Acompanhamentos, trazendo informações sobre os acompanhamentos solicitados. Como são 3 acompanhamentos, o processo foi realizado um de cada vez.

# Acompanhamento1
cond = [pedidos.Acomp1 == df_acompanhamento.IdAcompanhamento]
pedidos = pedidos.join(df_acompanhamento, cond, how='left').drop('Acomp1', 'IdAcompanhamento')
pedidos = pedidos.withColumnRenamed('NomAcompanhamento', 'Acompanhamento1')

# Acompanhamento2
cond = [pedidos.Acomp2 == df_acompanhamento.IdAcompanhamento]
pedidos = pedidos.join(df_acompanhamento, cond, how='left').drop('Acomp2', 'IdAcompanhamento')
pedidos = pedidos.withColumnRenamed('NomAcompanhamento', 'Acompanhamento2')

# Acompanhamento3
cond = [pedidos.Acomp3 == df_acompanhamento.IdAcompanhamento]
pedidos = pedidos.join(df_acompanhamento, cond, how='left').drop('Acomp3', 'IdAcompanhamento')
pedidos = pedidos.withColumnRenamed('NomAcompanhamento', 'Acompanhamento3')

# Selecionando colunas, renomeando e adicionando novas.

pedidos = pedidos.select(
    col('DataHora')
    ,to_date(col("DataHora"),"yyyy-MM-dd").alias("Data")
    ,hour(col('DataHora')).alias('HoraPedido')
    ,col('IdPedido')
    ,col('CategoriaCopo').alias('TamanhoCopo')
    ,col('VlrPrecoCopo').alias('Valor')
    ,col('NomCidade').alias('Cidade')
    ,col('NomEstado').alias('Estado')
    ,col('NomRegiao').alias('Regiao')
    ,col('NomEntrega').alias('TipoEntrega')
    ,col('NomPagamento').alias('TipoPagamento')
    ,col('NomCobertura').alias('Cobertura')
    ,col('Acompanhamento1')
    ,col('Acompanhamento2')
    ,col('Acompanhamento3')
)

# Criando coluna dt_ingestion para o particionamento
df_final = pedidos.withColumn("dt", current_date().cast(StringType()))

# Função para ingerir os dados na camada trusted
def carga_csv(arquivo):
    df.write.mode("overwrite")\
    .format("csv")\
    .save(f"s3a://datalake-dados/raw/{arquivo}")

arquivo = 'dataset_pedidos.csv'

# Executando a função
carga_parquet(arquivo)
print("Carga dos dados finalizada.")

# Finalizando a sessão spark
spark.stop()
