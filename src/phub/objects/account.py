from __future__ import annotations

import io
import os
import pandas as pd
import json
from pprint import pprint
from functools import cached_property
from typing import TYPE_CHECKING, Self, Literal, Iterator

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
        
        url = consts.HOST + f'/users/{self.name}'
        self.user = User(client = self.client, name = self.name, url = url)
        
        # We assert that the account is from a normal user (not model, etc.)
        if not 'users/' in self.user.url:
            logger.error(f'Invalid user type: {url}')
            #raise NotImplementedError('Non-user account are not supported.')
    
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
    def download_stats(self) -> bytes:  
        """
        Download the stats CSV file using curl or Selenium as fallback.

        Args:
            uname (str): Username
        
        Returns:
            bytes: Downloaded CSV file content
        """
     
        self.client.call(consts.PORNHUB_AUTHENTICATE_MAINHUB_REFER_URL, method = "get", timeout = 10) #we need the cookies from here 
        content = self.client.call(consts.PORNHUB_MAINHUB_EXPORT_URL, method = "get", timeout = 10).content
        df = pd.read_csv(io.StringIO(content.decode('utf-8')), sep=',')
        
        if not df.empty:
            csv_dir = os.path.join(consts.CWD, "csv")
            os.makedirs(csv_dir, exist_ok=True)
            csv_file_path = os.path.join(csv_dir, f"{self.client.credentials['username']+'.csv'}")
            df.to_csv(csv_file_path, index=False)
            logger.info(f"Downloaded stats CSV file of user: {self.client.credentials['username']}")
            self.client.db_ops.save_csv_data(df, self.client.credentials['username'])
            return df
        else:
            logger.error(f"Failed to download stats CSV file of user: {self.client.credentials['username']}")
            logger.debug(f"Response: {pprint(content)}")
            return False



    @logger.catch
    def get_json(self):
        """
        Get the JSON data of the model. 

        Args:
            username (str):

        Returns:
            dict: JSON data
        """
        self.client.call(consts.PORNHUB_AUTHENTICATE_MAINHUB_REFER_URL, timeout=10)
        res = self.client.call(consts.VIDEO_MANAGER_JSON, data='{"uc": 0, "itemsPerPage": 1000, "useOffset": 0}', timeout=10)
        data = json.loads(res.text)
        self.client.db_ops.save_json_data(data, self.client.credentials['username'])
        return data
    

# EOF