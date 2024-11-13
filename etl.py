import pandas as pd
from datetime import datetime
from db_config import get_engine  # Importa o engine do arquivo db_config

# Extração: Carregar os dados de vendas do arquivo Excel
def extract_data(file_path):
    try:
        df = pd.read_excel(file_path)  # Modificado para ler arquivos .xlsx
        print("Dados extraídos com sucesso!")
        return df
    except Exception as e:
        print(f"Erro ao extrair dados: {e}")
        return None

# Transformação: Limpar e manipular os dados
def transform_data(df):
    # Excluir vendas com quantidade negativa
    df = df[df["quantidade"] > 0]
    
    # Calcular o valor total de cada venda
    df["valor_total"] = df["quantidade"] * df["preco_unitario"]
    
    # Formatar a data para um formato mais legível
    df["data_formatada"] = df["data"].apply(lambda x: x.strftime("%d/%m/%Y"))  # Aplicação direta de strftime
    
    # Agregar dados: calcular o total de vendas por produto
    df_total_vendas = df.groupby("produto")["valor_total"].sum().reset_index()
    df_total_vendas = df_total_vendas.rename(columns={"valor_total": "total_vendas"})
    
    # Juntar o total de vendas de volta ao dataframe original
    df = df.merge(df_total_vendas, on="produto", how="left")
    
    print("Dados transformados com sucesso!")
    return df

# Carregamento: Salvar o dataframe transformado no banco de dados SQL
def load_to_sql(df, table_name):
    engine = get_engine()
    if engine is not None:
        try:
            # Carregar o dataframe para a tabela especificada
            df.to_sql(table_name, con=engine, if_exists="replace", index=False)
            print(f"Dados carregados com sucesso na tabela '{table_name}'")
        except Exception as e:
            print(f"Erro ao carregar dados para o banco de dados: {e}")

# Função principal para executar o pipeline ETL
def run_etl(input_file, table_name):
    # Etapa de Extração
    df = extract_data(input_file)
    if df is None:
        return
    
    # Etapa de Transformação
    df_transformed = transform_data(df)
    
    # Etapa de Carregamento no SQL
    load_to_sql(df_transformed, table_name)

# Executar o ETL
input_file = "data/PythonETL.xlsx"  # Arquivo de entrada no formato Excel
table_name = "vendas_transformadas"  # Nome da tabela no banco de dados
run_etl(input_file, table_name)
