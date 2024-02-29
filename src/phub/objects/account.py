from __future__ import annotations

import io
import os
import pandas as pd
import json
from pprint import pprint
from functools import cached_property
from typing import TYPE_CHECKING, Self, Literal, Iterator, Union
import concurrent.futures
from datetime import datetime, timedelta

from .. import utils
from .. import consts
from . import User, Image


if TYPE_CHECKING:
    from ..core import Client
    from . import Feed, queries, User

from ..consts import logger

class Account:
    '''
    Represents a connected Ponhub account,
    capable of accessing account-only features.
    If the login fails, there will be None.
    '''
    
    def __new__(cls, client: Client) -> Self | None:
        '''
        Check if the object creation is needed.
        
        Args:
            client (Client): The client that initialized this.
        
        Returns:
            Self: The Account object or None, if login is not triggered.
        '''
        
        if all(client.credentials.values()):
            logger.info('Creating new account object')
            return object.__new__(cls)
    
    def __init__(self, client: Client) -> None:
        '''
        Initialise a new account object.
        
        Args:
            client (Client): The client parent.
        '''
        
        self.client = client
        
        self.name: str = None
        self.avatar: Image = None
        self.is_premium: bool = None
        self.user: User = None
        self.model = Model(client)
        
        # Save data keys so far, so we can make a difference with the
        # cached property ones.
        self.loaded_keys = list(self.__dict__.keys()) + ['loaded_keys']
        
        logger.info(f'Account object {self} created')
        
    def __repr__(self) -> str:
        
        status = 'logged-out' if self.name is None else f'name={self.name}' 
        return f'phub.Account({status})'

    def connect(self, data: dict) -> None:
        '''
        Update account data once login was successful.
        
        Args:
            data (dict): Data fetched from the login request.
        '''
        
        self.name = data.get('username')
        self.avatar = Image(self.client, data.get('avatar'), name = 'avatar')
        self.is_premium = data.get('premium_redirect_cookie') != '0'

        url = consts.HOST + f'/{self.client.usertype}/{self.name}'
        self.user = User(client = self.client, name = self.name, url = url)
        
        
    
    def refresh(self, refresh_login: bool = False) -> None:
        '''
        Delete the object's cache to allow items refreshing.
        
        Args:
            refresh_login (bool): Whether to also attempt to re-log in.
        '''
        
        logger.info(f'Refreshing account {self}')
        
        if refresh_login:
            logger.info('Forcing login refresh')
            self.client.login(force = True)
        
        # Clear properties cache
        for key in list(self.__dict__.keys()):
            if not key in self.loaded_keys:
                
                logger.debug(f'Deleting key {key}')
                delattr(self, key)
    
    @cached_property
    def recommended(self) -> queries.VideoQuery:
        '''
        Videos recommended to the account.
        '''
        
        from . import queries
        
        return queries.VideoQuery(self.client, 'recommended')
    
    @cached_property
    def watched(self) -> queries.VideoQuery:
        '''
        Account video history.
        '''
        
        from . import queries
        
        return queries.VideoQuery(self.client, f'users/{self.name}/videos/recent')
    
    @cached_property
    def liked(self) -> queries.VideoQuery:
        '''
        Videos liked by the account.
        '''
        
        from . import queries
        
        return queries.VideoQuery(self.client, f'users/{self.name}/videos/favorites')
    
    @cached_property
    def subscriptions(self) -> Iterator[User]:
        '''
        Get the account subscriptions.
        '''
        
        page = self.client.call(f'users/{self.name}/subscriptions')
        
        for url, avatar in consts.re.get_users(page.text):
            
            obj = User.get(self.client, utils.concat(consts.HOST, url))
            obj._cached_avatar_url = avatar # Inject image url
            
            yield obj
    
    @cached_property
    def feed(self) -> Feed:
        '''
        The account feed.
        '''
        
        from . import Feed

        return Feed(self.client)
    
    def dictify(self,
                keys: Literal['all'] | list[str] = 'all',
                recursive: bool = False) -> dict:
        '''
        Convert the object to a dictionary.
        
        Args:
            keys (str): The data keys to include.
            recursive (bool): Whether to allow other PHUB objects to dictify.
        
        Returns:
            dict: Dict version of the object.
        '''
        
        return utils.dictify(self, keys, ['name', 'avatar',
                                          'is_premium', 'user'], recursive)
        
    

        
        

class Model(Account):
    
    def __init__(self, client: 'Client') -> None:
        self.client = client
    
    @logger.catch
    def get_stats_csv(self) -> pd.DataFrame | bool:  
        """
        Download the stats CSV file of the model/channel/pornstar.

        Args:
            uname (str): Username
        
        Returns:
            pd.DataFrame | bool: DataFrame containing the CSV data or False if failed.
        """
     
        self.client.call(consts.PORNHUB_GOTO_MAINHUB, timeout = 10) 
        content = self.client.call(consts.PORNHUB_MAINHUB_EXPORT_URL, timeout = 10).content
        df = pd.read_csv(io.StringIO(content.decode('utf-8')), sep=',')
        
        if not df.empty:
            
            if logger.level == 'DEBUG':
                # Save the CSV file locally
                csv_dir = os.path.join(consts.CWD, "csv")
                os.makedirs(csv_dir, exist_ok=True)
                self.csv_file_path = os.path.join(csv_dir, f"{self.client.credentials['username']+'.csv'}")
                df.to_csv(self.csv_file_path, index=False)
                logger.info(f"Downloaded stats CSV file of user: {self.client.credentials['username']}")
            
            # Save the CSV data to the database
            self.client.db_ops.save_csv_data(df, self.client.credentials['username'])
            return df
        
        else:
            logger.error(f"Failed to download stats CSV file of user: {self.client.credentials['username']}")
            logger.debug(f"Response: {pprint(content)}")
            return False



    @logger.catch
    def get_all_videos_json(self):
        """
        Get the JSON data of the model/channel/pornstar video manager for all videos. 

        Args:
            username (str):

        Returns:
            dict: JSON data
        """
        self.client.call(consts.PORNHUB_GOTO_MAINHUB, timeout=10)
        res = self.client.call(consts.VIDEO_MANAGER_JSON, method = 'POST', data = '{"uc": 0, "itemsPerPage": 2000, "useOffset": 0}', timeout = 10)
        data = json.loads(res.text)
        self.client.db_ops.save_video_json_data(data, self.client.credentials['username'])
        return data
    
    
    @logger.catch
    def get_daily_earnings_json(self) -> dict:
        """
        Get the JSON data of the daily earnings of the model/channel/pornstar.

        Args:
            username (str):

        Returns:
            dict: JSON data
        """
        self.client.call(consts.PORNHUB_GOTO_MAINHUB)
        res = self.client.call(consts.DAILY_EARNINGS_HISTORY_TEMPLATE.substitute(token = self.client._granted_token), timeout = 10)
        earnings_data = json.loads(res.text)
        self.client.db_ops.save_daily_earnings_data(earnings_data, self.client.credentials['username'])
        return earnings_data
    
    
    @logger.catch
    def get_payout_history_json(self) -> dict:
        """
        Get the JSON data of the payout history of the model/channel/pornstar.

        Args:
            username (str):

        Returns:
            dict: JSON data
        """
        # Get the mainhub page token
        page = self.client.call(consts.PORNHUB_GOTO_MAINHUB).text
        token = consts.re.token_mainhub(page)
        
        res = self.client.call(consts.TOTAL_PAYOUTS_HISTORY_TEMPLATE.substitute(token = token), timeout = 10)
        payout_data = json.loads(res.text)
        self.client.db_ops.save_payout_data(payout_data, self.client.credentials['username'])
        return payout_data
        
        
        
    @logger.catch
    def get_single_video_data_json(self) -> dict:
        """
        Get the JSON data of the model/channel/pornstar video manager.
        This is a timeseries that tells us how much money a video made on a given day.

        Returns:
            List: List of dicts 
        """

        def fetch_video_data(row):
            """
            Fetch the video data of each pornhub video (earnings and views timeseries).

            Args:
                row (pd.Series): A row of the filtered_ids DataFrame

            Returns:
                list: List of dictionaries containing the video data
            """
            video_url = row['Site URL']
            title = row['TITLE']
            video_id = row['ID'] if 'ID' in row else None
            res = self.client.call(consts.SINGLE_VIDEO_HISTORY_TEMPLATE.substitute(video_id = video_id, token = token), timeout = 10).text
            res = json.loads(res)
            
            video_data_list = []

            for data_type in ('views', 'sales'):
                if data_type in res['data']:
                    for item in res['data'][data_type]:
                        video_data = {
                            'timestamp': item['x'],
                            data_type: item['y'],
                            'site': item['site'],
                            'title': title,
                            'url': video_url,
                            'type': 'view' if data_type == 'views' else 'earnings'
                        }
                        video_data_list.append(video_data)
                        
            return video_data_list
        
        
        stats = self.get_stats_csv()
        filtered_ids = stats[stats['PORNHUB'] == 1][['ID', 'TITLE', 'Site URL']]

        # Get the mainhub page token
        page = self.client.call(consts.PORNHUB_GOTO_MAINHUB).text
        token = consts.re.token_mainhub(page)
        
        # Collect the video data using threads
        video_data_list = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_row = {executor.submit(fetch_video_data, row): row for _, row in filtered_ids.iterrows()}
            for future in concurrent.futures.as_completed(future_to_row):
                if not future.cancelled():
                    video_data_list.extend(future.result())  # Extend the main list with the results to flatten it

        
        self.client.db_ops.save_single_video_data(video_data_list, self.client.credentials['username'])
        return video_data_list

    
    @cached_property     
    def conversion_rate(self, year: int = None, month: int = None) -> Union[float, None]:
        # Create default values for year and month
        if year is None or month is None:
            current_date = datetime.now()
            first_day_of_current_month = current_date.replace(day=1)
            last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
            year = last_day_of_previous_month.year
            month = last_day_of_previous_month.month
            
        conversion_rate = self.client.db_ops.get_conversion_rate_for_payout(year=year, month=month, username=self.client.credentials['username'])
        if conversion_rate is not None:
            print(f"Conversion rate for {year}-{month} is: {conversion_rate}")
            return conversion_rate
        else:
            logger.error("Not enough data to calculate earnings per 1 million views.")
        

# EOF