'''
PHUB core module.
'''
import os
import time
import pyotp
import requests
from functools import cached_property

from . import utils
from . import consts
from . import errors
from . import locals
from .consts import logger
from .database import DatabaseOperations

from .modules import parser

from .objects import (Param, NO_PARAM, Video, User,
                      Account, Query, queries, Playlist)



class Client:
    '''
    Represents a client capable of handling requests
    with Pornhub.
    '''
    @logger.catch
    def __init__(self,
                 username: str = None,
                 password: str = None,
                 *,
                 language: str = 'en,en-US',
                 delay: int = 0,
                 proxies: dict = None,
                 login: bool = True,
                 two_factor_token: str = None) -> None:
        '''
        Initialise a new client.
        
        Args:
            username (str): Optional account username/address to connect to.
            password (str): Optional account password to connect to.
            language (str): Language locale (fr, en, ru, etc.)
            delay  (float): Minimum delay between requests.
            proxies (dict): Dictionary of proxies for the requests.
            login   (bool): Whether to automatically log in after initialization.
            two_factor_token (str): Optional two-factor token.
        '''
        
        logger.debug(f'Initialised new Client {self}')
        
        # Initialise session
        self.reset()
        
        self.proxies = proxies
        self.language = {'Accept-Language': language}
        self.credentials = {
            'username': username,
            'password': password,
        } 
        self.delay = delay
        self.start_delay = False
        
        # Connect account
        self.logged = False
        self.account = Account(self)
        logger.debug(f'Connected account to client {self.account}')
        
        # Database operations istantiation, if no db exists it will be created
        if username and password:
            users_dir = os.path.join(consts.CWD, 'users')
            os.makedirs(users_dir, exist_ok=True)
            db_path = os.path.join(users_dir, f"UserData.db")
            db_url = 'sqlite:///' + db_path.replace('\\', '/')
            self.db_ops = DatabaseOperations(str(db_url))
        
        # Automatic login
        if login and self.account:
            logger.debug('Automatic login triggered')
            self.login()
    
    def reset(self) -> None:
        '''
        Reset the client requests session.
        '''
        
        # Initialise session
        self.session = requests.Session()
        self._clear_granted_token()
        
        # Bypass age disclaimer
        self.session.cookies.set('accessAgeDisclaimerPH', '1')
        self.session.cookies.set('accessAgeDisclaimerUK', '1')
        self.session.cookies.set('accessPH', '1')
        self.session.cookies.set('age_verified', '1')
    
    def call(self,
             func: str,
             method: str = 'GET',
             data: dict = None,
             headers: dict = {},
             timeout: float = 30,
             throw: bool = True,
             silent: bool = False) -> requests.Response:
        '''
        Send a request.
        
        Args:
            func      (str): URL or PH function to call.
            method    (str): Request method.
            data     (dict): Optional data to send to the server.
            headers  (dict): Request optional headers.
            timeout (float): Request maximum response time.
            throw    (bool): Whether to raise an error when a request explicitly fails.
            silent   (bool): Make the call logging one level deeper.
        
        Returns:
            requests.Response: The fetched response.
        '''
        
        logger.log("DEBUG" if silent else "WARNING", f'Making call to {func or "/"}')
        
        # Delay
        if self.start_delay:
            time.sleep(self.delay)
        else:
            self.start_delay = True

        url = func if 'http' in func else utils.concat(consts.HOST, func)
        
        for i in range(consts.MAX_CALL_RETRIES):
            
            try:
                # Send request
                response = self.session.request(
                    method = method,
                    url = url,
                    headers = consts.HEADERS | headers | self.language,
                    data = data,
                    timeout = timeout,
                    proxies = self.proxies
                )
                
                # Silent 429 errors
                if b'429</title>' in response.content:
                    raise ConnectionError('Pornhub raised error 429: too many requests')
                
                # Attempt to resolve the challenge if needed
                if challenge := consts.re.get_challenge(response.text, False):
                    logger.info('\n\nChallenge found, attempting to resolve\n\n')
                    parser.challenge(self, *challenge)
                    continue # Reload page
                
                break
            
            except Exception as err:
                logger.log("DEBUG" if silent else "WARNING",
                           f'Call failed: {repr(err)}. Retrying (attempt {i + 1}/{consts.MAX_CALL_RETRIES})')
                time.sleep(consts.MAX_CALL_TIMEOUT)
                continue
        
        else:
            raise ConnectionError(f'Call failed after {i + 1} retries. Aborting.')

        if throw: response.raise_for_status()
        return response
    
    def login_cookies(self):
        """
        Log in using cookies from the database.

        Returns:
            - bool: Whether the login was successful.
        """
        logger.warning('Attempting to log in using cookies')
        session_cookies = self.db_ops.load_cookies(self.credentials['username'])
        if session_cookies:
            self.session.cookies.update(session_cookies)
            
        is_logged = self.call('front/authenticate', method='GET')
        data_is_logged = is_logged.json()
        logger.debug(f'Login cookies response: {data_is_logged}')
        
        if int(data_is_logged.get('success')) == 1:
            self.logged = True
            # Reset token
            self._clear_granted_token()
            # Update account data
            self.account.connect(is_logged)
        else:
            self.reset()
            
        self.logged = bool(data_is_logged.get('success'))
        return self.logged
            
    
    def login(self,
              force: bool = False,
              throw: bool = True) -> bool:
        '''
        Attempt to log in.
        
        Args:
            force (bool): Whether to force the login (used to reconnect).
            throw (bool): Whether to raise an error if this fails.
        
        Returns:
            bool: Whether the login was successful.
        '''
        
        logger.debug('Attempting login')
        
        if not force and self.logged:
            logger.error('Client is already logged in')
            raise errors.ClientAlreadyLogged()
        
        # Check if cookies are still valid and use cookies to log in if possible
        is_logged =  self.login_cookies()
        if is_logged:
            return True

        # Load credentials from database
        if not self.credentials['password']:
            logger.info('No password was provided, trying to load from database')
            self.credentials['username'], self.credentials['password'] = self.db_ops.load_credentials(self.credentials['username'])
        
        # Get token
        page = self.call('').text
        base_token = consts.re.get_token(page)
        
        # Send credentials
        payload = consts.LOGIN_PAYLOAD | self.credentials | {'token': base_token}
        response = self.call('front/authenticate', method = 'POST', data = payload)
        
        # Parse response
        data = response.json()
        success = int(data.get('success'))
        message = data.get('message')
        
        if success == 1:
            logger.info('Successfully logged in')
            self.logged = True
            return True
        elif success == 0 and data.get('autoLoginParameter'):
            # Handle 2FA
            token2 = data.get('autoLoginParameter')
            authy_id = data.get('authyId')
            verification_code = self.generate_otp(self.credentials['username'])  

            payload_2fa = {
                "authy_id": authy_id,
                "token": base_token,
                "token2": token2,
                "username": self.credentials['username'],
                "verification_code": verification_code,
                "verification_modal": "1"
            }

            response_2fa = self.call('front/authenticate', method='POST', data=payload_2fa)
            logger.debug(f'2FA response: {response_2fa.text}')

            # Parse 2FA response
            data_2fa = response_2fa.json()
            if int(data_2fa.get('success')) == 1:
                logger.info('Successfully logged in with 2FA')
                self.logged = True
                return True
            else: 
                if throw:
                    logger.error(f'Login with 2FA failed. Received error: {data_2fa}')
                    raise errors.LoginFailed('2FA authentication failed')
        
        if throw and not success:
            logger.error(f'Login failed: Received error: {message}')
            raise errors.LoginFailed(message)
        
        # Reset token
        self._clear_granted_token()
        
        # Update account data
        self.account.connect(data) if not data_2fa else self.account.connect(data_2fa)
        self.logged = bool(success)
        return self.logged
    
    def get(self, video: str | Video) -> Video:
        '''
        Fetch a Pornhub video.
        
        Args:
            video (str): Video full URL, partial URL or viewkey.
        
        Returns:
            Video: The corresponding video object.
        '''
        
        logger.debug('Fetching video at', video)

        if isinstance(video, Video):
            # User might want to re-init a video,
            # or use another client
            url = video.url
        
        elif 'http' in video:
            # Support full URLs
            url = video
        
        else:
            if 'key=' in video:
                # Support partial URLs
                key = video.split('key=')[1]
            
            else:
                # Support key only
                key = str(video)
            
            url = utils.concat(consts.HOST, 'view_video.php?viewkey=' + key)
        
        return Video(self, url)

    def get_user(self, user: str) -> User:
        '''
        Get a specific user.
        
        Args:
            user (str): user URL or name.
        
        Returns:
            User: The corresponding user object.
        '''
        
        logger.debug(f'Fetching user {user}')
        return User.get(self, user)

    def search(self,
               query: str,
               param: locals.constant = NO_PARAM,
               use_hubtraffic = True) -> Query:
        '''
        Performs searching on Pornhub.
        
        Args:
            query (str): The query to search.
            param (Param): Filters parameter.
            use_hubtraffic (bool): Whether to use the HubTraffic Pornhub API (faster but less precision).
        
        Returns:
            Query: Initialised query.
        '''
        
        # Assert a param type
        assert isinstance(param, Param)
        logger.info(f'Opening search query for `{query}`')
        
        # Assert sorting is compatible
        if (not (locals._allowed_sort_types in param)
            and locals._sort_period_types in param):
            
            raise errors.InvalidSortParam('Sort parameter not allowed')
        
        param_ = Param('search', query) | param
        
        if use_hubtraffic:
            return queries.JSONQuery(self, 'search', param_, query_repr = query)
        
        return queries.VideoQuery(self, 'video/search', param_, query_repr = query)
    
    def get_playlist(self, url: str = None):
        '''
        Initializes a Playlist object

        Args:
            url (str): The playlist url

        Returns:
            Playlist object
        '''

        if isinstance(url, Playlist):
            url = url.url

        return Playlist(self, url)

    def search_user(self,
                    username: str = None,
                    country: str = None,
                    city: str = None,
                    age: tuple[int] = None,
                    param: Param = NO_PARAM
                    ) -> queries.UserQuery:
        '''
        Search for users in the community.
        
        Args:
            username (str): The member username.
            country (str): The member **country code** (AF, FR, etc.)
            param (Param): Filters parameter.
        
        Returns:
            MQuery: Initialised query.
        
        '''
        
        params = (param
                  | Param('username', username)
                  | Param('city', city)
                  | Param('country', country))
        
        if age:
            params |= Param('age1', age[0])
            params |= Param('age2', age[1])
        
        return queries.UserQuery(self, 'user/search', params)
    
    def time_remaining_till_next_interval(self, timestep=30) -> int:
        """
        Calculate the time remaining (in seconds) until the next TOTP interval starts.

        Args:
        - timestep (int): The time step for TOTP, typically 30 seconds.

        Returns:
        - int: The number of seconds remaining.
        """
        elapsed_time = int(time.time()) % timestep
        return timestep - elapsed_time


    def generate_otp(self, username: str, timestep=30, wait_threshold=3) -> str:
        
        """
        Generate a TOTP for the given user.

        Raises:
            ValueError: If no secret key is found for the user.

        Returns:
            str: The generated OTP.
        """
        
        # Retrieve the remaining time until the next TOTP interval
        remaining_time = self.time_remaining_till_next_interval(timestep)

        # Wait until the next interval if necessary
        if remaining_time <= wait_threshold:
            time.sleep(remaining_time + 1)
            
        
        # Retrieve the secret key from the database
        secret_key = self.db_ops.get_secret_key(username)
        if secret_key is None:
            raise ValueError("No secret key found for user")

        # Generate the OTP using the secret key
        totp = pyotp.TOTP(secret_key, interval=timestep)
        return totp.now()
    
    def credentials_to_db(self, username: str, password: str, secret_key: str) -> None:
        """
        Adds or updates the credentials and secret key for a user in the database.

        Args:
            username (str): The username of the user.
            password (str): The password for the user.
            secret_key (str): The secret key for the user.
        """
        # Check if the database operations instance is available
        if not hasattr(self, 'db_ops'):
            logger.error("Database operations instance is not available.")
            return

        try:
            # Save the credentials using the DatabaseOperations instance
            self.db_ops.save_credentials(username, password)
            logger.info(f"Credentials for user '{username}' have been added/updated successfully.")

            # Save the secret key using the DatabaseOperations instance
            self.db_ops.insert_secret_key(username, secret_key)
            logger.info(f"Secret key for user '{username}' has been added/updated successfully.")
        except Exception as e:
            logger.error(f"Failed to add/update credentials and secret key for user '{username}'. Error: {e}")


    def _clear_granted_token(self) -> None:
        '''
        Clear the granted token cache.
        '''
        
        if '_granted_token' in self.__dict__:
            del self._granted_token

    @cached_property
    def _granted_token(self) -> str:
        '''
        Get a granted token after having
        authentified the account.
        '''
        
        assert self.logged, 'Client must be logged in'
        self._token_controller = True

        page = self.call('').text
        return consts.re.get_token(page)
    

# EOF
