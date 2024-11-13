from sqlalchemy import create_engine


DB_URL = "mysql+pymysql://root:senha!@localhost:3306/etl_teste" 

def get_engine():
    """Função para criar e retornar o engine de conexão com o banco de dados."""
    try:
        engine = create_engine(DB_URL)
        print("Conexão com o banco de dados estabelecida com sucesso!")
        return engine
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None
