import configparser
import pathlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# URI = 'postgresql://postgres:possword@localhost:5432/postgres'

file_config = pathlib.Path(__file__).parent.parent.joinpath('config.ini')
config = configparser.ConfigParser()
config.read(file_config)

user = config.get(section="DEV_DB", option="USER")
password = config.get(section="DEV_DB", option="PASSWORD")
domain = config.get(section="DEV_DB", option="DOMAIN")
port = config.get(section="DEV_DB", option="PORT")
db = config.get(section="DEV_DB", option="DB_NAME")

URI = f"postgresql://{user}:{password}@{domain}:{port}/{db}"

engine = create_engine(URI, echo=False, pool_size=5, max_overflow=0)
DBSession = sessionmaker(bind=engine)
session = DBSession()
