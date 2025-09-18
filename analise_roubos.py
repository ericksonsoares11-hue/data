from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, when, upper, trim

# Cria a sessão Spark
spark = SparkSession.builder.appName("MBA_BigData_FinalProject_RedHat").getOrCreate()

# --- Passo 1: Carregamento do Arquivo ---
file_path = "Roubo_de_Celular_Data_SSP.csv"

try:
    df = spark.read.csv(file_path, header=True, inferSchema=True, sep=';')
    
    initial_count = df.count()
    print("Total de registros no arquivo original:", initial_count)
    print("Esquema do DataFrame original:")
    df.printSchema()

    # --- Passo 2: Limpeza e Seleção de Colunas ---
    # Agora a lista de colunas inclui 'CIDADE'
    important_columns = [
        'DATAOCORRENCIA', 'HORAOCORRENCIA', 'PERIDOOCORRENCIA', 'CIDADE',
        'SEXO', 'IDADE', 'PROFISSAO',
        'MARCA_CELULAR', 'DELEGACIA_NOME'
    ]

    df_clean = df.select(important_columns)
    df_clean = df_clean.withColumn("DATAOCORRENCIA", to_date(col("DATAOCORRENCIA"), "dd/MM/yyyy"))
    df_clean = df_clean.withColumn("MARCA_CELULAR", upper(col("MARCA_CELULAR")))

    final_count = df_clean.count()
    print(f"\nTotal de registros após a limpeza: {final_count}")
    print(f"Número de registros removidos: {initial_count - final_count}")
    print("Esquema do DataFrame limpo:")
    df_clean.printSchema()

    # --- Passo 3: Geração das Vistas Analíticas ---

    print("\n--- 1. Top 10 Cidades com Mais Roubos ---")
    top_cidades = df_clean.groupBy("CIDADE").agg(
        count("*").alias("total_roubos")
    ).orderBy(col("total_roubos").desc()).limit(10)
    top_cidades.show(truncate=False)

    print("\n--- 2. Roubos por Período do Dia ---")
    roubos_por_periodo = df_clean.groupBy("PERIDOOCORRENCIA").agg(
        count("*").alias("total_roubos")
    ).orderBy(col("total_roubos").desc())
    roubos_por_periodo.show(truncate=False)

    print("\n--- 3. Roubos por Faixa Etária ---")
    df_faixas_etarias = df_clean.withColumn(
        "faixa_etaria",
        when(col("IDADE") < 18, "Menor de 18")
        .when((col("IDADE") >= 18) & (col("IDADE") <= 25), "18-25 anos")
        .when((col("IDADE") > 25) & (col("IDADE") <= 35), "26-35 anos")
        .when((col("IDADE") > 35) & (col("IDADE") <= 50), "36-50 anos")
        .when((col("IDADE") > 50) & (col("IDADE") <= 100), "50-100 anos")
        .otherwise("Não Informado")
    )
    roubos_por_idade = df_faixas_etarias.groupBy("faixa_etaria").agg(
        count("*").alias("total_roubos")
    ).orderBy(col("total_roubos").desc())
    roubos_por_idade.show(truncate=False)

    print("\n--- 4. Top 10 Marcas de Celular Mais Roubadas ---")
    top_marcas = df_clean.groupBy("MARCA_CELULAR").agg(
        count("*").alias("total_roubos")
    ).orderBy(col("total_roubos").desc()).limit(10)
    top_marcas.show(truncate=False)

except Exception as e:
    print(f"Ocorreu um erro no carregamento do arquivo: {e}")
