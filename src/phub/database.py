import pickle
from .consts import logger
import pickle

from sqlalchemy import create_engine, Column, String, LargeBinary, Integer, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, MetaData
from sqlalchemy.sql import text
from datetime import datetime
from typing import Union
import pandas as pd


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
    id = Column(Integer, primary_key=True, autoincrement=True)  
    username = Column(String, nullable=False) 
    timestamp = Column(DateTime, default=datetime.now)
    data = Column(JSON)


class DatabaseOperations:
    @logger.catch(level="DEBUG")
    def __init__(self, db_path: str):
        self.engine = create_engine(db_path, echo=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        
    @logger.catch(level="DEBUG")
    def save_credentials(self, username: str, password: str):
        """
        Save credentials for a user.

        Args:
            username (str):
            password (str): 
        """
        with self.Session() as session:
            credential = session.query(Credential).filter_by(username=username).first()
            if credential:
                credential.password = password
            else:
                credential = Credential(username=username, password=password)
                session.add(credential)
            session.commit()
            
    @logger.catch(level="DEBUG")
    def load_credentials(self, username: str) -> Union[tuple, None]:
        """
        Load credentials for a user.

        Args:
            username (str): 

        Returns:
            Union[tuple, None]: 
        """
        with self.Session() as session:
            credential = session.query(Credential).filter_by(username=username).first()
            
            if credential:
                logger.info(f"Credentials loaded for user: {username}")
                return credential.username, credential.password
            else:
                logger.info(f"No credentials found for user: {username}")
                return None
            
    @logger.catch(level="DEBUG")
    def save_cookies(self, username: str, cookies: dict):
        """
        Save cookies for a user.

        Args:
            username (str): 
            cookies (dict): 
        """
        with self.Session() as session:
            pickled_cookies = pickle.dumps(cookies)
            session_data = session.query(SessionData).filter_by(username=username).first()
            if session_data:
                session_data.session = pickled_cookies
            else:
                session_data = SessionData(username=username, session=pickled_cookies)
                session.add(session_data)
            session.commit()
            
            logger.info(f"Cookies saved for user: {username}")
            
    @logger.catch(level="DEBUG")
    def load_cookies(self, username: str) -> Union[dict, None]:
        """
        Load cookies for a user.

        Args:
            username (str): 

        Returns:
            Union[dict, None]: 
        """
        with self.Session() as session:
            session_data = session.query(SessionData).filter_by(username=username).first()
            
            if session_data:
                cookies = pickle.loads(session_data.session)
                logger.info(f"Cookies loaded for user: {username}")
                return cookies
            else:
                logger.warning(f"No cookies found for user: {username}")
                return None
            
    @logger.catch(level="DEBUG")
    def insert_secret_key(self, username: str, secret_key: str):
        """
        Insert secret key for a user.

        Args:
            username (str):
            secret_key (str): 
        """
        with self.Session() as session:
            secret_key_entry = session.query(SecretKey).filter_by(username=username).first()
            if secret_key_entry:
                secret_key_entry.secret_key = secret_key
            else:
                secret_key_entry = SecretKey(username=username, secret_key=secret_key)
                session.add(secret_key_entry)
            session.commit()
            
            logger.info(f"Secret key inserted for user: {username}")
            
    @logger.catch(level="DEBUG")
    def del_session(self, username: str):
        """
        Delete session for a user.

        Args:
            username (str): Username
        """
        with self.Session() as session:
            session_data = session.query(SessionData).filter_by(username=username).delete()
            session.commit()
            
            if session_data:
                logger.info(f"Session deleted for user: {username}")
            else:
                logger.info(f"No session found for user: {username}")
                
                
    @logger.catch(level="DEBUG")
    def maintain_last_3_timestamps(self):
        """ Maintain the last 3 timestamps in json_data table. """
        with self.Session() as session:
            subquery = session.query(JsonData.id).order_by(JsonData.timestamp.desc()).limit(3).subquery()
            session.query(JsonData).filter(~JsonData.id.in_(subquery)).delete(synchronize_session=False)
            session.commit()
        logger.info("Maintained the last 3 timestamps in json_data table")
        
        
        

    @logger.catch(level="DEBUG")
    def save_json_data(self, data: dict, username: str,):
        """ Save JSON data with current timestamp.
        Args:
            username (str): The username associated with the JSON data.
            data (dict): JSON data to save.
        """
        with self.Session() as session:
            json_data = JsonData(username=username, data=data)
            session.add(json_data)
            session.commit()
        logger.info("JSON data saved with current timestamp")
            
            
    @logger.catch(level="DEBUG")
    def get_secret_key(self, username: str) -> str:
        """
        Get secret key for a user.

        Args:
            username (str): 

        Returns:
            str: secret key
        """
        with self.Session() as session:
            secret_key_entry = session.query(SecretKey).filter_by(username=username).first()
            if secret_key_entry:
                logger.info(f"Secret key loaded for user: {username}")
                #logger.debug(f"Secret key: {secret_key_entry.secret_key}")
                return secret_key_entry.secret_key
            else:
                logger.info(f"No secret key found for user: {username}")
                return None
            
    @logger.catch(level="DEBUG")
    def save_csv_data(self, df: pd.DataFrame, username: str):
        """
        Save CSV data to the database dynamically creating the table structure, including a timestamp.

        Args:
            df (pd.DataFrame): DataFrame containing the CSV data.
        """
        
        # Dynamically create a table structure based on the CSV columns
        metadata = MetaData()
        csv_table = Table('csv_data', metadata,
                        *(Column(name, String) for name in df.columns),
                        Column('timestamp', DateTime, default=datetime.now),
                        Column('username', String))  

        # Create the table in the database if it doesn't exist
        metadata.create_all(self.engine)

        # Convert DataFrame to a list of dictionaries for bulk insert
        # Add the username to each row dictionary
        list_to_write = [dict(row, username=username) for row in df.to_dict(orient='records')]

        with self.Session() as session:
            # Insert data into the table
            session.execute(csv_table.insert(), list_to_write)
            session.commit()

        logger.info("CSV data saved to the database with timestamp.")
        
     