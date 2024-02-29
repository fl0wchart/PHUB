import pickle
from .consts import logger
import pickle

from sqlalchemy import UniqueConstraint, create_engine, Column, String, LargeBinary, Integer, DateTime, JSON, BigInteger, Float, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, MetaData
from sqlalchemy import func
from sqlalchemy.sql import text
from sqlalchemy.exc import IntegrityError
import json

from datetime import datetime, timedelta
from typing import Union
import pandas as pd
from typing import Type, List, Dict
import calendar


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
class VideoManager(Base):
    __tablename__ = 'video_manager'
    id = Column(Integer, primary_key=True, autoincrement=True)  
    username = Column(String, nullable=False) 
    timestamp = Column(DateTime, default=datetime.now)
    data = Column(JSON)
     
class VideoDataDaily(Base):
    __tablename__ = 'video_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, index=True)
    date: int = Column(BigInteger, nullable=False, index=True)
    views: int = Column(Integer)
    url: str = Column(String)
    title: str = Column(String)
    earnings: float = Column(Float)
    site: str = Column(String(255), nullable=False, index=True)
    type: str = Column(String(255))
    username: str = Column(String(255))

    # Create indexes for performance
    __table_args__ = (
        Index('ix_video_data_date', 'date'),
        Index('ix_video_data_timestamp', 'timestamp'),
        Index('ix_video_data_url', 'url'),
    )

class TotalEarningsDaily(Base):
    __tablename__ = 'total_earnings_daily'
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.now)
    earnings_type = Column(String(255))
    value = Column(Float)
    username = Column(String(255))
    
class PaymentInfo(Base):
    __tablename__ = 'payment_info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, nullable=False)
    finalized_on = Column(String, nullable=False)  # Using String to store the date in DDMMYYYY format
    net_amount = Column(Float, nullable=False)
    payment_status = Column(String, nullable=False)
    invoice_link = Column(String)





class DatabaseOperations:
    
    @logger.catch(level="DEBUG")
    def __init__(self, db_path: str):
        self.engine = create_engine(db_path, echo=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        
        
    
    # LOGIN OPERATIONS
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
                
                
                
                
                
    # DATA SAVING OPERATIONS
    @logger.catch(level="DEBUG")
    def save_video_json_data(self, data: dict, username: str,):
        """ 
        Save JSON data of video manager with current timestamp.
        We use this data just for monitoring video states by comparing the data with the previous one.

        Args:
            username (str): The username associated with the JSON data.
            data (dict): JSON data to save.
        """
        with self.Session() as session:
            json_data = VideoManager(username=username, data=data)
            session.add(json_data)
            session.commit()
        self.maintain_last_x_timestamps(VideoManager)
        logger.info("video manager data saved with current timestamp")
            
            
    @logger.catch(level="DEBUG")
    def save_csv_data(self, df: pd.DataFrame, username: str):
        """
        Save CSV data to the database dynamically creating the table structure, including a timestamp.
        Dynamically creates a table structure based on the CSV columns because we are lazy.
        We could save it as a JSON, but we want to be able to query the data.

        Args:
            df (pd.DataFrame): DataFrame containing the CSV data.
        """
        
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
        
     
    @logger.catch(level="DEBUG")
    def save_single_video_data(self, data_list: List[Dict], username: str) -> None:
        """
        Writes a list of video data dictionaries to the database.

        Args:
            data_list (List[Dict]): List of dictionaries containing video data.
        """
        timestamp = datetime.now()
        video_data_entries = []
        for data in data_list:
            video_data_entries.append(
                VideoDataDaily(
                    date=int(data.get('timestamp', 0)),
                    views=data.get('views', 0),
                    url=data.get('url', ''),
                    title=data.get('title', ''),
                    earnings=data.get('sales', 0.0),
                    site=data.get('site', ''),
                    type=data.get('type', ''),
                    username=username,
                    timestamp=timestamp
                )
            )

        with self.Session() as session:
            session.bulk_save_objects(video_data_entries)
            session.commit()
            self.maintain_last_x_timestamps(VideoDataDaily, 1)
            logger.info("Video data saved to the database.")

                
    @logger.catch
    def save_daily_earnings_data(self, data: dict, username: str) -> None:
        """
        Save the daily earnings timeseries

        Args:
            data (dict): The metrics data in JSON format.
            username (str): The username associated with the metrics data.
        """
        metrics_data_entries = []
        for date, values in data['data'].items():
            for site, site_data in values.items():
                for metric_type, metric_data in site_data.items():
                    amount = metric_data['amount']
                    metrics_data_entries.append(
                        TotalEarningsDaily(
                            timestamp=datetime.strptime(date, '%Y-%m-%d'),
                            earnings_type=metric_type,
                            value=amount,
                            username=username
                        )
                    )

        with self.Session() as session:
            session.bulk_save_objects(metrics_data_entries)
            session.commit()
            logger.info("Metrics data saved to the database.")

        
    @logger.catch(level="DEBUG")
    def save_payout_data(self, data: dict, username: str) -> None:
        """
        Writes payment information to the database.

        Args:
            data (dict): Dictionary containing payment information.
            username (str): Username associated with the payment information.
        """
        payment_info_list = data['data']['paymentInfo']

        with self.Session() as session:
            for payment_info in payment_info_list:
                # Convert the date to DDMMYYYY format
                finalized_on = datetime.strptime(payment_info['finalized_on'], '%B %d, %Y').strftime('%d%m%Y')

                payment_entry = PaymentInfo(
                    username=username,
                    finalized_on=finalized_on,
                    net_amount=payment_info['net_amount'],
                    payment_status=payment_info['payment_status'],
                    invoice_link=payment_info.get('invoice_link', ''),
                )
                session.add(payment_entry)
            session.commit()
        logger.info("Payment information saved to the database.")
        
        
        
        
        
    # DATA RETRIEVAL OPERATIONS
    @logger.catch(level="DEBUG")
    def get_conversion_rate_for_payout(self, year: int, month: int, username: str) -> float:
        """
        Calculate the earnings per 1 million views for a specific payout year and month, and username.
        We assume that the payouts are just for free videos which will be incorrect if bonuses etc. have been paid.

        Args:
            year (int): The year of the payout.
            month (int): The month of the payout (1-12).
            username (str): The username associated with the video data.

        Returns:
            float: The earnings per 1 million views.
        """
        # Calculate the start and end dates of the given month
        start_date = datetime(year, month, 1)
        end_date = start_date + timedelta(days=calendar.monthrange(year, month)[1])

        # Query the database for the video data in the given month
        with self.Session() as session:
            total_views = session.query(func.sum(VideoDataDaily.views)).filter(
                VideoDataDaily.username == username,
                VideoDataDaily.date >= start_date.timestamp(),
                VideoDataDaily.date < end_date.timestamp()
            ).scalar() or 0

            total_earnings = session.query(func.sum(VideoDataDaily.earnings)).filter(
                VideoDataDaily.username == username,
                VideoDataDaily.date >= start_date.timestamp(),
                VideoDataDaily.date < end_date.timestamp()
            ).scalar() or 0

        # Calculate the earnings per 1 million views
        if total_views > 0:
            earnings_per_million = f"{(total_earnings / total_views) * 1_000_000:,.2f}"
            return earnings_per_million
        else:
            return None
    
    
    @logger.catch(level="DEBUG")
    def get_monitor_data(self, username: str) -> List[str]:
        """
        Fetch the JSON data strings from the two most recent video entries for a given user.

        Args:
            username (str): The username of the user whose video entries' data are to be fetched.
        Returns:
            List[str]: A list containing the JSON data strings from the two most recent video entries for the user.
        Raises:
            Exception: If there are less than two video entries for the user.
        """
        with self.Session() as session:
            # Query the two most recent video entries for the given username
            rows = session.query(VideoManager.data).filter(VideoManager.username == username).order_by(VideoManager.timestamp.desc()).limit(2).all()
            
            if len(rows) < 2:
                logger.error(f"Not enough video data to compare for user {username}")
                raise Exception("Not enough video data to compare")
            
            # Extract the JSON data strings from the query result
            video_data_strings = [row[0] for row in rows]
            
            return video_data_strings
            
        
    # MAINTENANCE OPERATIONS
    @logger.catch(level="DEBUG")
    def maintain_last_x_timestamps(self, table_class, x: int = 10):
        """ Maintain the last x batches (timestamps) in the given table.
        Args:
            table_class (Base): The class representing the table to maintain the last x batches in.
            x (int): The number of batches to keep.
        """
        with self.Session() as session:
            # Get the last x unique batch_ids
            subquery = session.query(table_class.timestamp).distinct().order_by(table_class.timestamp.desc()).limit(x).subquery()
            
            # Delete records not in the last x batches
            session.query(table_class).filter(~table_class.timestamp.in_(subquery)).delete(synchronize_session=False)
            session.commit()
        logger.info(f"Kept only the last {x} batches in {table_class.__tablename__} table")
