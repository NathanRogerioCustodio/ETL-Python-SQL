from sqlalchemy import create_engine

# Configuração do banco de dados
DB_URL = "mysql+pymysql://root:N02r09c95!@localhost:3306/etl_teste"  # Substitua com suas informações

def get_engine():
    """Função para criar e retornar o engine de conexão com o banco de dados."""
    try:
        engine = create_engine(DB_URL)
        print("Conexão com o banco de dados estabelecida com sucesso!")
        return engine
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None
