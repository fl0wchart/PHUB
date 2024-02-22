import pickle
from .consts import logger
import pickle
from .utils import catch_all_exceptions
from sqlalchemy import create_engine, Column, String, LargeBinary, Integer, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from typing import Union


Base = declarative_base()

# Define models
# Not really secret, encryption is needed here
class Credential(Base):
    __tablename__ = 'credentials'
    username = Column(String, primary_key=True)
    password = Column(String)

class SessionData(Base):
    __tablename__ = 'session_data'
    username = Column(String, primary_key=True)
    session = Column(LargeBinary)

# Not really secret, encryption is needed here
class SecretKey(Base):
    __tablename__ = 'secret_keys'
    username = Column(String, primary_key=True)
    secret_key = Column(String)

# Table to store JSON data with timestamp of model account video manager
class JsonData(Base):
    __tablename__ = 'json_data'
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.now().isoformat())
    data = Column(JSON)


# Database operations class
@catch_all_exceptions #use for debugging
class DatabaseOperations:
    def __init__(self, db_path: str):
        self.engine = create_engine(db_path, echo=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def save_credentials(self, username: str, password: str):
        with self.Session() as session:
            credential = session.query(Credential).filter_by(username=username).first()
            if credential:
                credential.password = password
            else:
                credential = Credential(username=username, password=password)
                session.add(credential)
            session.commit()
            session.close()

    def load_credentials(self, username: str) -> Union[tuple, None]:
        with self.Session() as session:
            credential = session.query(Credential).filter_by(username=username).first()
            session.close()
            if credential:
                logger.info(f"Credentials loaded for user: {username}")
                return credential.username, credential.password
            else:
                logger.info(f"No credentials found for user: {username}")
                return None

    def save_cookies(self, username: str, cookies: dict):
        with self.Session() as session:
            pickled_cookies = pickle.dumps(cookies)
            session_data = session.query(SessionData).filter_by(username=username).first()
            if session_data:
                session_data.session = pickled_cookies
            else:
                session_data = SessionData(username=username, session=pickled_cookies)
                session.add(session_data)
            session.commit()
            session.close()
            logger.info(f"Cookies saved for user: {username}")

    def load_cookies(self, username: str) -> Union[dict, None]:
        with self.Session() as session:
            session_data = session.query(SessionData).filter_by(username=username).first()
            session.close()
            if session_data:
                cookies = pickle.loads(session_data.session)
                logger.info(f"Cookies loaded for user: {username}")
                return cookies
            else:
                logger.info(f"No cookies found for user: {username}")
                return None

    def insert_secret_key(self, username: str, secret_key: str):
        with self.Session() as session:
            secret_key_entry = session.query(SecretKey).filter_by(username=username).first()
            if secret_key_entry:
                secret_key_entry.secret_key = secret_key
            else:
                secret_key_entry = SecretKey(username=username, secret_key=secret_key)
                session.add(secret_key_entry)
            session.commit()
            session.close()
            logger.info(f"Secret key inserted for user: {username}")

    def del_session(self, username: str):
        with self.Session() as session:
            session_data = session.query(SessionData).filter_by(username=username).delete()
            session.commit()
            session.close()
            if session_data:
                logger.info(f"Session deleted for user: {username}")
            else:
                logger.info(f"No session found for user: {username}")

    def maintain_last_3_timestamps(self):
        with self.Session() as session:
            subquery = session.query(JsonData.id).order_by(JsonData.id.desc()).limit(3).subquery()
            session.query(JsonData).filter(~JsonData.id.in_(subquery)).delete(synchronize_session=False)
            session.commit()
            session.close()
            logger.info("Maintained the last 3 timestamps in json_data table")

    def save_json_data(self, data):
        with self.Session() as session:
            json_data = JsonData(timestamp=datetime.now().isoformat(), data=data)
            session.add(json_data)
            session.commit()
            session.close()
            logger.info("JSON data saved with current timestamp")
        
    def get_secret_key(self, username: str) -> str:
        with self.Session() as session:
            secret_key_entry = session.query(SecretKey).filter_by(username=username).first()
            if secret_key_entry:
                logger.info(f"Secret key loaded for user: {username}")
                return secret_key_entry.secret_key
            else:
                logger.info(f"No secret key found for user: {username}")
                return None