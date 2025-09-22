POSTGRES_DB: str = "flight_db"
POSTGRES_PASSWORD: str = "airflow_password"
POSTGRES_USER: str = "airflow_user"
POSTGRES_HOST: str = "localhost"
POSTGRES_PORT: int = 5432
_schema: str = "flight_data"

postgres_connection_string = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
