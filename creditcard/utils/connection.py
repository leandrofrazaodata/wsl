import os
import psycopg2
from psycopg2 import sql, OperationalError
from dotenv import load_dotenv

class PostgresDB:
    def __init__(self):
        # Carregar variáveis de ambiente do arquivo .env
        load_dotenv()
        
        self.dbname = os.getenv('postgres_database')
        self.user = os.getenv('postgres_username')
        self.password = os.getenv('postgres_pass')
        self.host = os.getenv('postgres_hostname')
        self.port = int(os.getenv('DB_PORT', 5432))  # Usa 5432 como valor padrão se não definido
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establishes the connection to the database."""
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            print("Connection to the database established successfully.")
        except OperationalError as e:
            print(f"Error while connecting to PostgreSQL: {e}")

    def disconnect(self):
        """Closes the connection to the database."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connection to the database closed.")

    def execute_query(self, query, params=None):
        """Executes a query on the database."""
        try:
            if self.cursor:
                self.cursor.execute(sql.SQL(query), params)
                self.connection.commit()
                print("Query executed successfully.")
            else:
                print("No active connection.")
        except Exception as e:
            print(f"Error executing query: {e}")

    def fetch_results(self):
        """Fetches all results from the last executed query."""
        try:
            if self.cursor:
                return self.cursor.fetchall()
            else:
                print("No active connection.")
                return None
        except Exception as e:
            print(f"Error fetching results: {e}")
            return None

# Exemplo de uso:
if __name__ == "__main__":
    db = PostgresDB()

    db.connect()

    # Executar uma consulta
    db.execute_query("SELECT * FROM persons.customer;")
    results = db.fetch_results()
    if results:
        for row in results:
            print(row)

    db.disconnect()
