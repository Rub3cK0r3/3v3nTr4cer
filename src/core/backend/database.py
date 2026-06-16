from sqlmodel import create_engine
from dotenv import load_dotenv
from urllib.parse import quote_plus
import os

# The Database data should always
# be loaded from your execution environment.
# Please, bear that in mind.

# I load the .env used in the docker compose deployment..

load_dotenv(dotenv_path='../../db/.env')

db_user = os.environ.get("POSTGRES_USER", "devuser")
db_passwd = quote_plus(os.environ.get("POSTGRES_PASSWORD", "devpass"))
db_host = os.environ.get("DB_HOST", "db")
db_name = os.environ.get("POSTGRES_DB", "eventdb")

DATABASE_URL = f"postgresql://{db_user}:{db_passwd}@{db_host}:5432/{db_name}"
engine = create_engine(DATABASE_URL, echo=True)
