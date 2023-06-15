from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import os
import psycopg2 as db

def ler_dados_csv_minas_gerais(): 
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    df = pd.read_csv(AIRFLOW_HOME + '/dags/obitos-confirmados-covid-19-minas-gerais.csv', sep=';', index_col=0, low_memory=False)

    # OBSERVANDO A QUANTIDADE DE REGISTROS NULOS
    print(df.isnull().sum())

    # REMOVENDO VALORES NULOS...
    df = df.dropna()

    # REMOVENDO VALORES 'Não informado' DA COLUNA COMORBIDADE
    df = df.loc[df['COMORBIDADE'] != 'Não informado']

    #SALVANDO NO BANCO DE DADOS
    data = []
    for index, row in df.iterrows():
        data.append((row["SEXO"], row["IDADE"], row["MUNICIPIO_RESIDENCIA"], 'MG', row["DATA_OBITO"], row["COMORBIDADE"]))

    data_for_db = tuple(data)

    conn_string="dbname='pos' host='172.18.0.3' user='airflow' password='airflow'"
    conn=db.connect(conn_string)
    cur=conn.cursor()
    query = "insert into obitos_covid (sexo, idade, municipio, estado, data_obito, comorbidade) values(%s,%s,%s,%s,%s,%s)"

    cur.executemany(query,data_for_db)
    conn.commit()

def ler_dados_csv_alagoas():
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    df = pd.read_csv(AIRFLOW_HOME + '/dags/obitos-confirmados-covid-19-alagoas.csv', sep=';', encoding='ISO-8859-1', index_col=0, low_memory=False)

    # RENOMEANDO COLUNAS
    df = df.rename(columns={'Data do Óbito (Caso haja)': 'DATA_OBITO', 
                            'MUNICÍPIO': 'MUNICIPIO_RESIDENCIA', 
                            'Comorbidades': 'COMORBIDADE'})
    
    # REMOVENDO COLUNAS DESNECESSÁRIAS
    df = df.drop(columns=['Data de atendimento no Serviço', 
                          'CLASSIFICAÇÃO (Confirmado, suspeito, descartado, óbito, curado)', 
                          'Situação do paciente confirmado (UTI, isolamento domiciliar, enfermaria) ?', 
                          'Data de confirmação', 
                          'Unnamed: 15'])
    
    # OBSERVANDO A QUANTIDADE DE REGISTROS NULOS
    print(df.isnull().sum())
    
    # REMOVENDO VALORES NULOS...
    df = df.dropna()

    # FILTRANDO DADOS DE ÓBITOS DO ANO 2020
    df = df.loc[(df['DATA_OBITO'] >= '2020-01-01') & (df['DATA_OBITO'] < '2020-12-31')]

    #SALVANDO NO BANCO DE DADOS
    data = []
    for index, row in df.iterrows():
        comorbidade = 'NÃO' if row["COMORBIDADE"] == 'SEM COMORBIDADE' else 'SIM'
        sexo = 'M' if row["SEXO"] == 'M' or row["SEXO"] == 'm' else 'F'
        data.append((sexo, row["IDADE"], row["MUNICIPIO_RESIDENCIA"], 'AL', row["DATA_OBITO"], comorbidade))

    data_for_db = tuple(data)

    conn_string="dbname='pos' host='172.18.0.3' user='airflow' password='airflow'"
    conn=db.connect(conn_string)
    cur=conn.cursor()
    query = "insert into obitos_covid (sexo, idade, municipio, estado, data_obito, comorbidade) values(%s,%s,%s,%s,%s,%s)"

    cur.executemany(query,data_for_db)
    conn.commit()

with DAG('dag_pos', start_date = datetime(2023,6,1), 
         schedule_interval = '@once', catchup = False) as dag:
    
    criar_tabela_obitos_covid = PostgresOperator (
        task_id = 'criar_tabela_obitos_covid',
        postgres_conn_id = 'postgres-airflow',
        sql = """
            create table if not exists obitos_covid (
            id serial primary key,
            sexo varchar,
            idade int,
            municipio varchar,
            estado varchar,
            data_obito date,
            comorbidade varchar
        )
        """
    )

    criar_tabela_meses_ano = PostgresOperator (
        task_id = 'criar_tabela_meses_ano',
        postgres_conn_id = 'postgres-airflow',
        sql = """
            CREATE TABLE if not exists meses_ano (
                id serial PRIMARY KEY,
                numero_mes int unique,
                nome_mes varchar unique
            )
        """
    )

    inserir_dados_tabela_meses_ano = PostgresOperator (
        task_id = 'inserir_dados_tabela_meses_ano',
        postgres_conn_id = 'postgres-airflow',
        sql = """
            INSERT INTO meses_ano(numero_mes, nome_mes)
            VALUES(1, 'Janeiro'), (2, 'Fevereiro'), (3, 'Março'), (4, 'Abril'), (5, 'Maio'), (6, 'Junho'), 
                (7, 'Julho'), (8, 'Agosto'), (9, 'Setembro'), (10, 'Outubro'), (11, 'Novembro'), (12, 'Dezembro') 
            ON CONFLICT DO NOTHING;
        """
    )

    leitura_dados_minas_gerais = PythonOperator (
        task_id = 'leitura_dados_minas_gerais',
        python_callable = ler_dados_csv_minas_gerais
    )

    leitura_dados_alagoas = PythonOperator (
        task_id = 'leitura_dados_alagoas',
        python_callable = ler_dados_csv_alagoas
    )

criar_tabela_obitos_covid >> criar_tabela_meses_ano >> inserir_dados_tabela_meses_ano >> leitura_dados_minas_gerais >> leitura_dados_alagoas