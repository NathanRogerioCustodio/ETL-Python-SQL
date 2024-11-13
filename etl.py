import pandas as pd
from datetime import datetime
from db_config import get_engine  


def extract_data(file_path):
    try:
        df = pd.read_excel(file_path)  
        print("Dados extraídos com sucesso!")
        return df
    except Exception as e:
        print(f"Erro ao extrair dados: {e}")
        return None


def transform_data(df):
    
    df = df[df["quantidade"] > 0]
    
    
    df["valor_total"] = df["quantidade"] * df["preco_unitario"]
    
    
    df["data_formatada"] = df["data"].apply(lambda x: x.strftime("%d/%m/%Y"))  
    
    
    df_total_vendas = df.groupby("produto")["valor_total"].sum().reset_index()
    df_total_vendas = df_total_vendas.rename(columns={"valor_total": "total_vendas"})
    
    
    df = df.merge(df_total_vendas, on="produto", how="left")
    
    print("Dados transformados com sucesso!")
    return df


def load_to_sql(df, table_name):
    engine = get_engine()
    if engine is not None:
        try:
            
            df.to_sql(table_name, con=engine, if_exists="replace", index=False)
            print(f"Dados carregados com sucesso na tabela '{table_name}'")
        except Exception as e:
            print(f"Erro ao carregar dados para o banco de dados: {e}")


def run_etl(input_file, table_name):
  
    df = extract_data(input_file)
    if df is None:
        return
    
   
    df_transformed = transform_data(df)
    
    
    load_to_sql(df_transformed, table_name)


input_file = "data/PythonETL.xlsx"  
table_name = "vendas_transformadas"  
run_etl(input_file, table_name)
