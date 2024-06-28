# %%
import os
import json


# %%
# Importando blibliotecas pyspark
from pyspark.sql import SparkSession


# %%
# Demais bibliotecas
import requests
import pandas as pd
import findspark
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy import text
from sqlalchemy.sql import delete
from sqlalchemy.orm import sessionmaker

findspark.init()
# %%
# Bliblioteca que configura as variáveis de ambiente do spark pra nós


# Configura a variável de ambiente PYARROW_IGNORE_TIMEZONE
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
# %%
# Credenciais para acesso a API
load_dotenv()
app_id = os.environ.get("APP_ID")
access_key = os.environ.get("ACCESS_KEY")
table_name = os.environ.get("TABLE_NAME_1")

# %%
# Inicializa sessão Spark
spark_session = (
    SparkSession.builder.appName("API_APPSHEET").master("local[*]").getOrCreate()
)

spark_session


# %%
# URL da API AppSheet para ler registros
api_url = f"https://api.appsheet.com/api/v2/apps/{app_id}/tables/{table_name}/Action"

# Corpo da solicitação
payload = {
    "Action": "Find",
    "Properties": {"Locale": "en-US", "Timezone": "Pacific Standard Time"},
    "Rows": [],
}

# Cabeçalho da solicitação com a chave de acesso do aplicativo
headers = {"applicationAccessKey": access_key, "Content-Type": "application/json"}

try:
    # Fazendo a solicitação POST
    response = requests.post(api_url, json=payload, headers=headers)

    # Verifica se a solicitação foi bem-sucedida (código 200)
    if response.status_code == 200:
        # Retorna os dados da resposta (formatados como JSON)
        json_data = response.json()
    else:
        # Imprime uma mensagem de erro se a solicitação falhou
        print(f"Erro na solicitação. Código de Resposta: {response.status_code}")

# Tratamento de erro:
except json.JSONDecodeError as e:
    print(f"Erro ao decodificar JSON: {e}")
    print(
        f"Resposta da API: {response.text}"
    )  # Imprime o conteúdo da resposta para debug
except requests.RequestException as e:
    print(f"Erro na requisição HTTP: {e}")


# %%
df = pd.DataFrame(json_data)
df.to_csv("dados/cadastro_funcionarios.csv")

# %%
df_spark = spark_session.read.csv(
    "dados/cadastro_funcionarios.csv", header="true", inferSchema="true"
)

# %%
# Renomeando colunas uma a uma
df_spark = (
    df_spark.withColumnRenamed("_RowNumber", "index")
    .withColumnRenamed("Row ID", "id")
    .withColumnRenamed("Nome", "nome")
    .withColumnRenamed("RG", "rg")
    .withColumnRenamed("CPF", "cpf")
    .withColumnRenamed("Coordenador", "coordenador")
    .withColumnRenamed("Senioridade", "senioridade")
    .withColumnRenamed("Classificação", "classificacao")
    .withColumnRenamed("Tipo Contratação", "tipo_de_contratacao")
    .withColumnRenamed("Área", "area")
    .withColumnRenamed("Indicado Por", "indicado_por")
    .withColumnRenamed("Gênero", "genero")
    .withColumnRenamed("Restrições/Observações", "observacoes")
    .withColumnRenamed("Telefone", "telefone")
    .withColumnRenamed("Telefone 2", "telefone_2")
    .withColumnRenamed("Data de Nascimento", "data_de_nascimento")
    .withColumnRenamed("Data Aceite", "data_aceite")
    .withColumnRenamed("Início da Jornada", "inicio_da_jornada")
    .withColumnRenamed("Fim da Jornada", "fim_da_jornada")
    .withColumnRenamed("Idade", "idade")
    .withColumnRenamed("Bairro", "bairro")
    .withColumnRenamed("Endereço", "endereco")
    .withColumnRenamed("Número", "numero")
    .withColumnRenamed("CEP", "cep")
    .withColumnRenamed("Cidade", "cidade")
    .withColumnRenamed("Estado", "estado")
    .withColumnRenamed("CNPJ", "cnpj")
    .withColumnRenamed("Nome da Empresa", "nome_da_empresa")
    .withColumnRenamed("Tam. Camiseta", "tam_camiseta")
    .withColumnRenamed("Filhos", "filhos")
    .withColumnRenamed("Quantos?", "quantos")
    .withColumnRenamed("Entrevistador RH", "entrevistador_rh")
    .withColumnRenamed("Entrevistador Técnico", "entrevistador_tecnico")
    .withColumnRenamed("Status", "status")
    .withColumnRenamed("Motivo do Status", "motivo_do_status")
    .withColumnRenamed("Motivo Saída", "motivo_da_saida")
    .withColumnRenamed("E-mail", "email")
    .withColumnRenamed("Função", "funcao")
    .withColumnRenamed("Foto", "foto")
    .withColumnRenamed("Gerente", "gerente")
    .withColumnRenamed("Age", "age")
    .withColumnRenamed("QuantidadeDias", "tempo_de_empresa_em_dias")
    .withColumnRenamed("Tempo de Empresa", "tempo_de_empresa")
)


# %%
# Dropando colunas
df_spark = df_spark.drop(
    "_c0",
    "Ações",
    "Complemento",
    "Crachá",
    "Observação/Anotações",
    "CPF_Mask",
    "Ausências",
    "Histórico 1:1",
    "Soft Skills",
    "Aniversário?",
    "Histórico Coordenação",
    "Histórico Áreas",
    "Histórico de Ocorrências",
    "Exceções de Apontamento",
    "Weeklys",
    "Histórico Funções",
    "Aniversariantes do mês",
    "Mês de aniversário",
)


# %%
# Criando uma engine com SQLAlchemy para BigQuery
engine = create_engine("bigquery://", credentials_path="keyfile.json")

Session = sessionmaker(engine)
session = Session()
metadata = MetaData()

# Definindo tabela e Schema
table_name = "bronze_cadastro_funcionarios"
dataset_name = "bronze"

#
cadastro_funcionarios = Table(
    table_name,
    metadata,
    Column("index", Integer),
    Column("id", String, primary_key=True),
    Column("nome", String),
    Column("coordenador", String),
    Column("senioridade", String),
    Column("classificacao", String),
    Column("tipo_de_contratacao", String),
    Column("area", String),
    Column("indicado_por", String),
    Column("genero", String),
    Column("observacoes", String),
    Column("telefone", String),
    Column("telefone_2", String),
    Column("data_de_nascimento", String),
    Column("data_aceite", String),
    Column("inicio_da_jornada", String),
    Column("fim_da_jornada", String),
    Column("idade", Integer),
    Column("estado", String),
    Column("cidade", String),
    Column("bairro", String),
    Column("endereco", String),
    Column("numero", String),
    Column("cep", String),
    Column("rg", String),
    Column("cpf", String),
    Column("cnpj", String),
    Column("nome_da_empresa", String),
    Column("tam_camiseta", String),
    Column("filhos", String),
    Column("quantos", Integer),
    Column("entrevistador_rh", String),
    Column("entrevistador_tecnico", String),
    Column("status", String),
    Column("motivo_do_status", String),
    Column("motivo_da_saida", String),
    Column("email", String),
    Column("funcao", String),
    Column("cat_pag", String),
    Column("foto", String),
    Column("gerente", String),
    Column("age", Integer),
    Column("tempo_de_empresa", String),
    Column("tempo_de_empresa_em_dias", Integer),
    schema=dataset_name,
)

# Criando tabela
metadata.create_all(engine, checkfirst=True)


# Deletando dados (caso exista)
delete_dados = delete(cadastro_funcionarios).where(text("1=1"))
session.execute(delete_dados)
session.commit()
session.close()


# %%
df_pandas = df_spark.toPandas()
df_pandas["telefone_2"] = df_pandas["telefone_2"].astype(str)
df_pandas["quantos"] = df_pandas["quantos"].fillna(0).astype(int)
df_pandas["data_de_nascimento"] = df_pandas["data_de_nascimento"].astype(str)
df_pandas["data_aceite"] = df_pandas["data_aceite"].astype(str)
df_pandas["inicio_da_jornada"] = df_pandas["inicio_da_jornada"].astype(str)
df_pandas["fim_da_jornada"] = df_pandas["fim_da_jornada"].astype(str)

# %%
# Escrevendo o DataFrame Pandas para o PostgreSQL
df_pandas.to_sql(table_name, engine, schema="bronze", if_exists="append", index=False)
print("Dados incluídos com sucesso")
# %%
spark_session.stop()
