
import re
import asyncio
import logging
import json
import signal
import sys
import sqlite3
import csv
import os
from datetime import datetime
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from telethon import Button

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TelegramCardExtractor:
    def __init__(self, api_id, api_hash, bot_token):
        self.API_ID = api_id
        self.API_HASH = api_hash
        self.BOT_TOKEN = bot_token
        
        # Configuration
        self.ADMIN_ID = 7577966487 # Replace with your Telegram user ID
        self.SOURCE_GROUP_ID = -1002682944548
        self.TARGET_CHANNELS = [-4829612702]
        
        # Clients
        self.client = None
        self.user_client = None
        
        # State management
        self.user_states = {}
        self.session_string = None
        self.is_monitoring = False
        
        # Sent cards tracking (to prevent duplicates)
        self.recently_sent_cards = set()
        
        # Database setup
        self.setup_database()
        
        # Load saved session
        self.load_session_from_file()

    def setup_database(self):
        """Setup SQLite database for storing processed messages and sessions"""
        try:
            self.conn = sqlite3.connect('bot_data.db', check_same_thread=False)
            self.cursor = self.conn.cursor()
            
            # Create tables
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS processed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER,
                    group_id INTEGER,
                    processed_at TEXT,
                    UNIQUE(message_id, group_id)
                )
            ''')
            
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_string TEXT,
                    created_at TEXT,
                    is_active INTEGER DEFAULT 1
                )
            ''')
            
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS extracted_cards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    card_data TEXT,
                    source_message_id INTEGER,
                    extracted_at TEXT,
                    UNIQUE(card_data)
                )
            ''')
            
            self.conn.commit()
            logger.info("\u2705 Database setup complete")
            
        except Exception as e:
            logger.error("\u274c Database setup failed: {}".format(e))

    def save_session_to_file(self, session_string):
        """Save session string to file"""
        try:
            with open('session.json', 'w') as f:
                json.dump({
                    'session': session_string,
                    'saved_at': datetime.now().isoformat()
                }, f, indent=2)
            logger.info("\ud83d\udcbe Session saved to file")
        except Exception as e:
            logger.error("\u274c Failed to save session to file: {}".format(e))

    def load_session_from_file(self):
        """Load session string from file"""
        try:
            with open('session.json', 'r') as f:
                data = json.load(f)
                self.session_string = data.get('session')
                if self.session_string:
                    logger.info("\ud83d\udcc2 Session loaded from file")
        except FileNotFoundError:
            logger.info("\ud83d\udcc2 No session file found")
        except Exception as e:
            logger.error("\u274c Failed to load session from file: {}".format(e))

    async def save_session_to_db(self, session_string):
        """Save session to database"""
        try:
            self.cursor.execute(
                'INSERT INTO sessions (session_string, created_at) VALUES (?, ?)',
                (session_string, datetime.now().isoformat())
            )
            self.conn.commit()
            logger.info("\ud83d\udcbe Session saved to database")
        except Exception as e:
            logger.error("\u274c Failed to save session to database: {}".format(e))

    def is_admin(self, user_id):
        """Check if user is admin"""
        return user_id == self.ADMIN_ID

    async def validate_session(self, session_string):
        """Validate session string by attempting to connect"""
        try:
            logger.info("\ud83d\udd0d Validating session string...")
            test_client = TelegramClient(StringSession(session_string), self.API_ID, self.API_HASH)
            
            await test_client.start()
            me = await test_client.get_me()
            await test_client.disconnect()
            
            if me:
                logger.info("\u2705 Session validation successful for {}".format(me.first_name))
                return True
            else:
                logger.error("\u274c Session validation failed - no user info")
                return False
                
        except SessionPasswordNeededError:
            logger.error("\u274c Session validation failed - 2FA required")
            return False
        except Exception as e:
            logger.error("\u274c Session validation failed: {}".format(e))
            return False

    async def setup_user_client(self, session_string):
        """Setup user client with session string"""
        try:
            if self.user_client and self.user_client.is_connected():
                await self.user_client.disconnect()
            
            self.user_client = TelegramClient(
                StringSession(session_string), 
                self.API_ID, 
                self.API_HASH
            )
            
            await self.user_client.start()
            
            # Start monitoring
            await self.start_monitoring()
            self.is_monitoring = True
            
            logger.info("\ud83d\ude80 User client setup successful")
            return True
            
        except Exception as e:
            logger.error("\u274c User client setup failed: {}".format(e))
            return False

    def extract_credit_cards(self, text):
        """Extract credit card information from text using enhanced regex patterns"""
        if not text:
            return []
        
        # Enhanced regex patterns for different card formats
        patterns = [
            # Format: 1234567890123456 12/25 123
            r'(\d{13,19})\s+(\d{1,2})/(\d{2,4})\s+(\d{3,4})',
            
            # Format: 1234567890123456|12|25|123
            r'(\d{13,19})\|(\d{1,2})\|(\d{2,4})\|(\d{3,4})',
            
            # Format: 1234567890123456 | 12 | 25 | 123
            r'(\d{13,19})\s*\|\s*(\d{1,2})\s*\|\s*(\d{2,4})\s*\|\s*(\d{3,4})',
            
            # Format: 1234567890123456/12/25/123
            r'(\d{13,19})[\/\-\s](\d{1,2})[\/\-\s](\d{2,4})[\/\-\s](\d{3,4})',
            
            # Format: 1234 5678 9012 3456 12/25 123
            r'(\d{4})\s*(\d{4})\s*(\d{4})\s*(\d{4})\s+(\d{1,2})/(\d{2,4})\s+(\d{3,4})',
            
            # Format: 1234-5678-9012-3456 12/25 123
            r'(\d{4})-(\d{4})-(\d{4})-(\d{4})\s+(\d{1,2})/(\d{2,4})\s+(\d{3,4})',
            
            # Format: 1234567890123456:12:25:123
            r'(\d{13,19}):(\d{1,2}):(\d{2,4}):(\d{3,4})',
            
            # Format: 1234567890123456 12 25 123
            r'(\d{13,19})\s+(\d{1,2})\s+(\d{2,4})\s+(\d{3,4})',
            
            # Format: 1234567890123456\u202212\u202225\u2022123
            r'(\d{13,19})\u2022(\d{1,2})\u2022(\d{2,4})\u2022(\d{3,4})',
            
            # Format: 1234567890123456>12>25>123
            r'(\d{13,19})>(\d{1,2})>(\d{2,4})>(\d{3,4})',
            
            # Format: 1234567890123456;12;25;123
            r'(\d{13,19});(\d{1,2});(\d{2,4});(\d{3,4})',
            
            # Format: 1234 5678 9012 3456;12;25;123
            r'(\d{4})\s*(\d{4})\s*(\d{4})\s*(\d{4});(\d{1,2});(\d{2,4});(\d{3,4})',
            
            # Format: 1234 5678 9012 3456:12:25:123
            r'(\d{4})\s*(\d{4})\s*(\d{4})\s*(\d{4}):(\d{1,2}):(\d{2,4}):(\d{3,4})',
            
            # Format: 1234567890123456=12=25=123
            r'(\d{13,19})=(\d{1,2})=(\d{2,4})=(\d{3,4})',
            
            # Format: 1234567890123456 - 12/25 - 123
            r'(\d{13,19})\s*-\s*(\d{1,2})/(\d{2,4})\s*-\s*(\d{3,4})',
            
            # Format: 1234567890123456 ~ 12/25 ~ 123
            r'(\d{13,19})\s*~\s*(\d{1,2})/(\d{2,4})\s*~\s*(\d{3,4})',
            
            # Format: 1234567890123456 (12/25) (123)
            r'(\d{13,19})\s*\((\d{1,2})/(\d{2,4})\)\s*\((\d{3,4})\)',
            
            # Format: 1234567890123456 12/25 cvv 123
            r'(\d{13,19})\s+(\d{1,2})/(\d{2,4})\s+(?:cvv|cvc|ccv|cid)\s*:?\s*(\d{3,4})',
            
            # Format: 1234567890123456 exp 12/25 cvv 123
            r'(\d{13,19})\s+(?:exp|expiry|expiration)\s*:?\s*(\d{1,2})/(\d{2,4})\s+(?:cvv|cvc|ccv|cid)\s*:?\s*(\d{3,4})',
            
            # Format: cc: 1234567890123456 exp: 12/25 cvv: 123
            r'(?:cc|card)\s*:?\s*(\d{13,19})\s+(?:exp|expiry|expiration)\s*:?\s*(\d{1,2})/(\d{2,4})\s+(?:cvv|cvc|ccv|cid)\s*:?\s*(\d{3,4})',
            
            # Format: 1234 5678 9012 3456 - exp: 12/25 - cvv: 123
            r'(\d{4})\s*(\d{4})\s*(\d{4})\s*(\d{4})\s*-\s*(?:exp|expiry|expiration)\s*:?\s*(\d{1,2})/(\d{2,4})\s*-\s*(?:cvv|cvc|ccv|cid)\s*:?\s*(\d{3,4})',
            
            # NEW PATTERNS
            
            # Format: 1234567890123456|09/26|123
            r'(\d{13,19})\|(\d{1,2})\/(\d{2,4})\|(\d{3,4})',
            
            # Format: 1234567890123456|09|26|123
            r'(\d{13,19})\|(\d{1,2})\|(\d{2})\|(\d{3,4})',
            
            # Format: 1234567890123456|09|2026|123
            r'(\d{13,19})\|(\d{1,2})\|(\d{4})\|(\d{3,4})',
            
            # Format: 1234567890123456|09/2026|123
            r'(\d{13,19})\|(\d{1,2})\/(\d{4})\|(\d{3,4})',
            
            # Format: 1234567890123456 09/26 123
            r'(\d{13,19})\s+(\d{1,2})\/(\d{2,4})\s+(\d{3,4})',
            
            # Format: 1234567890123456 09-26 123
            r'(\d{13,19})\s+(\d{1,2})\-(\d{2,4})\s+(\d{3,4})',
            
            # Format: 1234567890123456 09.26 123
            r'(\d{13,19})\s+(\d{1,2})\.(\d{2,4})\s+(\d{3,4})',
            
            # Format: 1234567890123456 09_26 123
            r'(\d{13,19})\s+(\d{1,2})\_(\d{2,4})\s+(\d{3,4})',
            
            # Format: 1234567890123456 (09/26) (123)
            r'(\d{13,19})\s+\((\d{1,2})\/(\d{2,4})\)\s+\((\d{3,4})\)',
            
            # Format: 1234567890123456 (09-26) (123)
            r'(\d{13,19})\s+\((\d{1,2})\-(\d{2,4})\)\s+\((\d{3,4})\)',
            
            # Format: 1234567890123456 [09/26] [123]
            r'(\d{13,19})\s+\[(\d{1,2})\/(\d{2,4})\]\s+\[(\d{3,4})\]',
            
            # Format: 1234567890123456 [09-26] [123]
            r'(\d{13,19})\s+\[(\d{1,2})\-(\d{2,4})\]\s+\[(\d{3,4})\]',
            
            # Format: 1234567890123456 {09/26} {123}
            r'(\d{13,19})\s+\{(\d{1,2})\/(\d{2,4})\}\s+\{(\d{3,4})\}',
            
            # Format: 1234567890123456 {09-26} {123}
            r'(\d{13,19})\s+\{(\d{1,2})\-(\d{2,4})\}\s+\{(\d{3,4})\}',
            
            # Format: 1234567890123456 <09/26> <123>
            r'(\d{13,19})\s+<(\d{1,2})\/(\d{2,4})>\s+<(\d{3,4})>',
            
            # Format: 1234567890123456 <09-26> <123>
            r'(\d{13,19})\s+<(\d{1,2})\-(\d{2,4})>\s+<(\d{3,4})>',
            
            # Format: 1234567890123456 09/26 cvv123
            r'(\d{13,19})\s+(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid)(\d{3,4})',
            
            # Format: 1234567890123456 09-26 cvv123
            r'(\d{13,19})\s+(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid)(\d{3,4})',
            
            # Format: 1234567890123456 09/26 cvv:123
            r'(\d{13,19})\s+(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid):(\d{3,4})',
            
            # Format: 1234567890123456 09-26 cvv:123
            r'(\d{13,19})\s+(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid):(\d{3,4})',
            
            # Format: 1234567890123456 09/26 cvv=123
            r'(\d{13,19})\s+(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid)=(\d{3,4})',
            
            # Format: 1234567890123456 09-26 cvv=123
            r'(\d{13,19})\s+(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid)=(\d{3,4})',
            
            # Format: 1234567890123456 09/26 cvv-123
            r'(\d{13,19})\s+(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid)\-(\d{3,4})',
            
            # Format: 1234567890123456 09-26 cvv-123
            r'(\d{13,19})\s+(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid)\-(\d{3,4})',
            
            # Format: 1234567890123456 09/26 123 (cvv)
            r'(\d{13,19})\s+(\d{1,2})\/(\d{2,4})\s+(\d{3,4})\s+\((?:cvv|cvc|ccv|cid)\)',
            
            # Format: 1234567890123456 09-26 123 (cvv)
            r'(\d{13,19})\s+(\d{1,2})\-(\d{2,4})\s+(\d{3,4})\s+\((?:cvv|cvc|ccv|cid)\)',
            
            # Format: 1234567890123456 exp:09/26 cvv:123
            r'(\d{13,19})\s+(?:exp|expiry|expiration):(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid):(\d{3,4})',
            
            # Format: 1234567890123456 exp:09-26 cvv:123
            r'(\d{13,19})\s+(?:exp|expiry|expiration):(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid):(\d{3,4})',
            
            # Format: 1234567890123456 exp=09/26 cvv=123
            r'(\d{13,19})\s+(?:exp|expiry|expiration)=(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid)=(\d{3,4})',
            
            # Format: 1234567890123456 exp=09-26 cvv=123
            r'(\d{13,19})\s+(?:exp|expiry|expiration)=(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid)=(\d{3,4})',
            
            # Format: 1234567890123456 exp-09/26 cvv-123
            r'(\d{13,19})\s+(?:exp|expiry|expiration)\-(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid)\-(\d{3,4})',
            
            # Format: 1234567890123456 exp-09-26 cvv-123
            r'(\d{13,19})\s+(?:exp|expiry|expiration)\-(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid)\-(\d{3,4})',
            
            # Additional patterns for more formats
            # Format: 4319474049322295|09/26|017
            r'(\d{13,19})\|(\d{1,2})\/(\d{2,4})\|(\d{3,4})',
            
            # Format: 4319474049322295|09|26|017
            r'(\d{13,19})\|(\d{1,2})\|(\d{2})\|(\d{3,4})',
            
            # Format: 4319474049322295|09|2026|017
            r'(\d{13,19})\|(\d{1,2})\|(\d{4})\|(\d{3,4})',
            
            # Format: 4319474049322295|09/2026|017
            r'(\d{13,19})\|(\d{1,2})\/(\d{4})\|(\d{3,4})',
            
            # Format: 4319474049322295 09/26 017
            r'(\d{13,19})\s+(\d{1,2})\/(\d{2,4})\s+(\d{3,4})',
            
            # Format: 4319474049322295 09-26 017
            r'(\d{13,19})\s+(\d{1,2})\-(\d{2,4})\s+(\d{3,4})',
            
            # Format: 4319474049322295 09.26 017
            r'(\d{13,19})\s+(\d{1,2})\.(\d{2,4})\s+(\d{3,4})',
            
            # Format: 4319474049322295 09_26 017
            r'(\d{13,19})\s+(\d{1,2})\_(\d{2,4})\s+(\d{3,4})',
            
            # Format: 4319474049322295 (09/26) (017)
            r'(\d{13,19})\s+\((\d{1,2})\/(\d{2,4})\)\s+\((\d{3,4})\)',
            
            # Format: 4319474049322295 (09-26) (017)
            r'(\d{13,19})\s+\((\d{1,2})\-(\d{2,4})\)\s+\((\d{3,4})\)',
            
            # Format: 4319474049322295 [09/26] [017]
            r'(\d{13,19})\s+\[(\d{1,2})\/(\d{2,4})\]\s+\[(\d{3,4})\]',
            
            # Format: 4319474049322295 [09-26] [017]
            r'(\d{13,19})\s+\[(\d{1,2})\-(\d{2,4})\]\s+\[(\d{3,4})\]',
            
            # Format: 4319474049322295 {09/26} {017}
            r'(\d{13,19})\s+\{(\d{1,2})\/(\d{2,4})\}\s+\{(\d{3,4})\}',
            
            # Format: 4319474049322295 {09-26} {017}
            r'(\d{13,19})\s+\{(\d{1,2})\-(\d{2,4})\}\s+\{(\d{3,4})\}',
            
            # Format: 4319474049322295 <09/26> <017>
            r'(\d{13,19})\s+<(\d{1,2})\/(\d{2,4})>\s+<(\d{3,4})>',
            
            # Format: 4319474049322295 <09-26> <017>
            r'(\d{13,19})\s+<(\d{1,2})\-(\d{2,4})>\s+<(\d{3,4})>',
            
            # Format: 4319474049322295 09/26 cvv017
            r'(\d{13,19})\s+(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid)(\d{3,4})',
            
            # Format: 4319474049322295 09-26 cvv017
            r'(\d{13,19})\s+(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid)(\d{3,4})',
            
            # Format: 4319474049322295 09/26 cvv:017
            r'(\d{13,19})\s+(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid):(\d{3,4})',
            
            # Format: 4319474049322295 09-26 cvv:017
            r'(\d{13,19})\s+(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid):(\d{3,4})',
            
            # Format: 4319474049322295 09/26 cvv=017
            r'(\d{13,19})\s+(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid)=(\d{3,4})',
            
            # Format: 4319474049322295 09-26 cvv=017
            r'(\d{13,19})\s+(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid)=(\d{3,4})',
            
            # Format: 4319474049322295 09/26 cvv-017
            r'(\d{13,19})\s+(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid)\-(\d{3,4})',
            
            # Format: 4319474049322295 09-26 cvv-017
            r'(\d{13,19})\s+(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid)\-(\d{3,4})',
            
            # Format: 4319474049322295 09/26 017 (cvv)
            r'(\d{13,19})\s+(\d{1,2})\/(\d{2,4})\s+(\d{3,4})\s+\((?:cvv|cvc|ccv|cid)\)',
            
            # Format: 4319474049322295 09-26 017 (cvv)
            r'(\d{13,19})\s+(\d{1,2})\-(\d{2,4})\s+(\d{3,4})\s+\((?:cvv|cvc|ccv|cid)\)',
            
            # Format: 4319474049322295 exp:09/26 cvv:017
            r'(\d{13,19})\s+(?:exp|expiry|expiration):(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid):(\d{3,4})',
            
            # Format: 4319474049322295 exp:09-26 cvv:017
            r'(\d{13,19})\s+(?:exp|expiry|expiration):(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid):(\d{3,4})',
            
            # Format: 4319474049322295 exp=09/26 cvv=017
            r'(\d{13,19})\s+(?:exp|expiry|expiration)=(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid)=(\d{3,4})',
            
            # Format: 4319474049322295 exp=09-26 cvv=017
            r'(\d{13,19})\s+(?:exp|expiry|expiration)=(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid)=(\d{3,4})',
            
            # Format: 4319474049322295 exp-09/26 cvv-017
            r'(\d{13,19})\s+(?:exp|expiry|expiration)\-(\d{1,2})\/(\d{2,4})\s+(?:cvv|cvc|ccv|cid)\-(\d{3,4})',
            
            # Format: 4319474049322295 exp-09-26 cvv-017
            r'(\d{13,19})\s+(?:exp|expiry|expiration)\-(\d{1,2})\-(\d{2,4})\s+(?:cvv|cvc|ccv|cid)\-(\d{3,4})',
        ]
        
        credit_cards = []
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.MULTILINE | re.IGNORECASE)
            
            for match in matches:
                try:
                    if len(match) == 4:
                        # Standard format: card, month, year, cvv
                        card_number, month, year, cvv = match
                        card_number = re.sub(r'[\s\-]', '', card_number)
                    elif len(match) == 7:
                        # Spaced format: card1, card2, card3, card4, month, year, cvv
                        if all(len(part) == 4 for part in match[:4]):
                            # Format with 4 card parts
                            card1, card2, card3, card4, month, year, cvv = match
                            card_number = card1 + card2 + card3 + card4
                        else:
                            # Format with 3 card parts and other info
                            continue
                    else:
                        continue
                    
                    # Validate card number length
                    if len(card_number) < 13 or len(card_number) > 19:
                        continue
                    
                    # Basic Luhn algorithm check
                    if not self.is_valid_card_number(card_number):
                        continue
                    
                    # Validate month
                    try:
                        month_int = int(month)
                        if not (1 <= month_int <= 12):
                            continue
                    except ValueError:
                        continue
                    
                    # Normalize year format
                    if len(year) == 4:
                        year = year[-2:]
                    elif len(year) != 2:
                        continue
                    
                    # Validate CVV
                    if len(cvv) < 3 or len(cvv) > 4:
                        continue
                    
                    # Format as number|month|year|cvv
                    formatted_card = f"{card_number}|{month.zfill(2)}|{year}|{cvv}"
                    credit_cards.append(formatted_card)
                    
                except Exception as e:
                    logger.error("Error processing card match: {}".format(e))
                    continue
        
        # Remove duplicates while preserving order
        seen = set()
        unique_cards = []
        for card in credit_cards:
            if card not in seen:
                seen.add(card)
                unique_cards.append(card)
        
        return unique_cards

    def is_valid_card_number(self, card_number):
        """
        Validate card number using Luhn algorithm
        """
        try:
            # Remove any non-digit characters
            digits = [int(d) for d in card_number if d.isdigit()]
            
            # Luhn algorithm
            checksum = 0
            for i, digit in enumerate(reversed(digits)):
                if i % 2 == 1:  # Odd position (0-indexed from the right)
                    digit *= 2
                    if digit > 9:
                        digit -= 9
                checksum += digit
            
            return checksum % 10 == 0
        except Exception:
            return False

    async def is_card_already_sent(self, card_data):
        """Check if card was already sent recently"""
        # Check in-memory cache first
        if card_data in self.recently_sent_cards:
            return True
        
        # Check database
        try:
            self.cursor.execute(
                'SELECT 1 FROM extracted_cards WHERE card_data = ?',
                (card_data,)
            )
            result = self.cursor.fetchone() is not None
            
            # If not in database, add to in-memory cache
            if not result:
                self.recently_sent_cards.add(card_data)
                # Limit cache size
                if len(self.recently_sent_cards) > 1000:
                    self.recently_sent_cards.pop()
            
            return result
        except Exception as e:
            logger.error("Error checking if card was already sent: {}".format(e))
            return False

    async def send_to_target_channels(self, card_data):
        """Send extracted card data to target channels using the bot client"""
        if not self.client or not self.client.is_connected():
            logger.error("\u274c Bot client not connected")
            return False
        
        # Just send the card data without BIN info
        message = " {}".format(card_data)
        
        success_count = 0
        
        for channel_id in self.TARGET_CHANNELS:
            try:
                # Send using bot client instead of user client
                await self.client.send_message(channel_id, message)
                logger.info("\u2705 Sent card {}*** to channel {}".format(card_data[:12], channel_id))
                success_count += 1
                
                # Add delay to avoid rate limiting
                await asyncio.sleep(0.5)
                
            except FloodWaitError as e:
                logger.warning("\u23f0 Flood wait error: {} seconds".format(e.seconds))
                await asyncio.sleep(e.seconds)
                
            except Exception as e:
                logger.error("\u274c Failed to send to channel {}: {}".format(channel_id, e))
        
        return success_count > 0

    async def process_message_for_cards(self, event):
        """Process message for credit card extraction"""
        try:
            message_id = event.message.id
            
            # Check if already processed
            if await self.is_message_processed(message_id, self.SOURCE_GROUP_ID):
                return
            
            # Get message text
            text = event.message.message or ""
            
            # Also check replied message if exists
            if hasattr(event.message, 'reply_to') and event.message.reply_to:
                try:
                    replied_msg = await event.message.get_reply_message()
                    if replied_msg and replied_msg.message:
                        text = f"{text} {replied_msg.message}"
                except Exception as e:
                    logger.error("Error getting replied message: {}".format(e))
            
            if not text.strip():
                return
            
            logger.info("\ud83d\udd0d Processing message {}: {}...".format(message_id, text[:100]))
            
            # Extract credit cards
            credit_cards = self.extract_credit_cards(text)
            
            if not credit_cards:
                logger.info("\u274c No credit cards found in message {}".format(message_id))
                return
            
            logger.info("\ud83d\udcb3 Found {} credit cards in message {}".format(len(credit_cards), message_id))
            
            # Process each card
            for card_data in credit_cards:
                try:
                    # Check if card was already sent (deduplication)
                    if await self.is_card_already_sent(card_data):
                        logger.info("\u23ed\ufe0f Skipping duplicate card: {}***".format(card_data[:12]))
                        continue
                    
                    logger.info("\ud83d\udce4 Processing card: {}***".format(card_data[:12]))
                    
                    # Send to target channels (without BIN info)
                    success = await self.send_to_target_channels(card_data)
                    
                    if success:
                        # Save to database
                        await self.save_extracted_card(card_data, message_id)
                    
                    # Add delay between cards
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error("\u274c Error processing card {}: {}".format(card_data[:12], e))
            
            # Mark message as processed
            await self.mark_message_processed(message_id, self.SOURCE_GROUP_ID)
            
        except Exception as e:
            logger.error("\u274c Error processing message {}: {}".format(event.message.id, e))

    async def save_extracted_card(self, card_data, message_id):
        """Save extracted card to database"""
        try:
            self.cursor.execute(
                'INSERT OR IGNORE INTO extracted_cards (card_data, source_message_id, extracted_at) VALUES (?, ?, ?)',
                (card_data, message_id, datetime.now().isoformat())
            )
            self.conn.commit()
        except Exception as e:
            logger.error("Error saving extracted card: {}".format(e))

    async def is_message_processed(self, message_id, group_id):
        """Check if message was already processed"""
        try:
            self.cursor.execute(
                'SELECT 1 FROM processed_messages WHERE message_id = ? AND group_id = ?',
                (message_id, group_id)
            )
            return self.cursor.fetchone() is not None
        except Exception as e:
            logger.error("Error checking processed message: {}".format(e))
            return False

    async def mark_message_processed(self, message_id, group_id):
        """Mark message as processed"""
        try:
            self.cursor.execute(
                'INSERT OR IGNORE INTO processed_messages (message_id, group_id, processed_at) VALUES (?, ?, ?)',
                (message_id, group_id, datetime.now().isoformat())
            )
            self.conn.commit()
        except Exception as e:
            logger.error("Error marking message as processed: {}".format(e))

    async def start_monitoring(self):
        """Start monitoring the source group for new messages"""
        if not self.user_client:
            logger.error("\u274c User client not available")
            return
        
        @self.user_client.on(events.NewMessage(chats=[self.SOURCE_GROUP_ID]))
        async def message_handler(event):
            if self.is_monitoring:
                await self.process_message_for_cards(event)
        
        logger.info("\ud83d\udd0d Started monitoring group {}".format(self.SOURCE_GROUP_ID))

    async def stop_monitoring(self):
        """Stop monitoring"""
        self.is_monitoring = False
        if self.user_client and self.user_client.is_connected():
            try:
                await self.user_client.disconnect()
                logger.info("\ud83d\uded1 Monitoring stopped")
            except Exception as e:
                logger.error("Error stopping monitoring: {}".format(e))

    async def start_bot(self):
        """Start the Telegram bot"""
        try:
            # Initialize bot client
            self.client = TelegramClient('bot_session', self.API_ID, self.API_HASH)
            await self.client.start(bot_token=self.BOT_TOKEN)
            
            logger.info("\ud83e\udd16 Bot client started")
            
            # Setup bot handlers
            await self.setup_bot_handlers()
            
            # Auto-start monitoring if session exists
            if self.session_string:
                logger.info("\ud83d\udd04 Auto-starting with existing session...")
                success = await self.setup_user_client(self.session_string)
                if success:
                    logger.info("\u2705 Auto-start successful")
                else:
                    logger.warning("\u26a0\ufe0f Auto-start failed - session may be invalid")
            
            logger.info("\ud83d\ude80 Bot is ready and running!")
            await self.client.run_until_disconnected()
            
        except Exception as e:
            logger.error("\u274c Bot startup failed: {}".format(e))
            raise

    async def setup_bot_handlers(self):
        """Setup all bot command and message handlers"""
        
        @self.client.on(events.NewMessage(pattern='/start'))
        async def start_handler(event):
            try:
                user_id = event.sender_id
                
                # Create the menu button
                menu_button = Button.inline("Menu", "menu")
                
                # Simple welcome message with button
                welcome_text = "\ud83e\udd16 Card Extractor Bot"
                
                await event.respond(welcome_text, buttons=[menu_button])
                
            except Exception as e:
                logger.error("Error in start handler: {}".format(e))
        
        @self.client.on(events.CallbackQuery(data="menu"))
        async def menu_callback(event):
            try:
                user_id = event.sender_id
                
                # Check if user is admin
                if not self.is_admin(user_id):
                    await event.answer("( -_\u2022)\u2584\ufe3b\u30c7\u2550\u2550\u2501\u4e00 \u2603\ufe0f", alert=True)
                    return
                
                # Admin menu with better formatting
                commands_text = (
                    "Available Commands:\
"
                    "\
"
                    "\u2022 /addstring - Add session string\
"
                    "\
"
                    "\u2022 /status - Check bot status\
"
                    "\
"
                    "\u2022 /stop - Stop monitoring\
"
                    "\
"
                    "\u2022 /restart - Restart monitoring\
"
                    "\
"
                    "\u2022 /stats - View statistics\
"
                    "\
"
                    "Use /addstring to begin monitoring."
                )
                
                # Add back button
                back_button = Button.inline("Back", "back")
                
                # Edit the original message instead of sending a new one
                await event.edit(commands_text, buttons=[back_button])
                
            except Exception as e:
                logger.error("Error in menu callback: {}".format(e))

        @self.client.on(events.CallbackQuery(data="back"))
        async def back_callback(event):
            try:
                # Simple welcome message with button
                welcome_text = "\ud83e\udd16 Card Extractor Bot"
                menu_button = Button.inline("Menu", "menu")
                
                # Edit the message to go back to the main screen
                await event.edit(welcome_text, buttons=[menu_button])
                
            except Exception as e:
                logger.error("Error in back callback: {}".format(e))

        @self.client.on(events.NewMessage(pattern='/addstring'))
        async def addstring_handler(event):
            if not self.is_admin(event.sender_id):
                return
            
            logger.info("\ud83d\udd11 Addstring command from admin {}".format(event.sender_id))
            
            self.user_states[event.sender_id] = {'step': 'awaiting_session'}
            
            await event.respond(
                "\ud83d\udd11 **Session String Required**\
"
                "\
"
                "Please send your Telegram session string in the next message.\
"
                "\
"
                "**How to get session string:**\
"
                "1. Run session generator script\
"
                "2. Login with your account\
"
                "3. Copy the generated string\
"
                "4. Send it here\
"
                "\
"
                "**Note:** Session string should be a long encoded string starting with something like '1BVtsO..'"
            )

        @self.client.on(events.NewMessage(pattern='/status'))
        async def status_handler(event):
            if not self.is_admin(event.sender_id):
                return
            
            logger.info("\ud83d\udcca Status command from admin {}".format(event.sender_id))
            
            # Get statistics
            try:
                self.cursor.execute('SELECT COUNT(*) FROM processed_messages')
                processed_count = self.cursor.fetchone()[0]
                
                self.cursor.execute('SELECT COUNT(*) FROM extracted_cards')
                extracted_count = self.cursor.fetchone()[0]
                
                self.cursor.execute('SELECT COUNT(*) FROM extracted_cards WHERE DATE(extracted_at) = DATE("now")')
                today_count = self.cursor.fetchone()[0]
                
            except Exception as e:
                logger.error("Error getting statistics: {}".format(e))
                processed_count = extracted_count = today_count = 0
            
            status_text = (
                "Bot Status :\
"
                "\
"
                "\ud83d\udd0d Monitoring: {} Active\
"
                "\
"
                "\ud83d\udd17 User Client: {} Connected\
"
                "\
"
                "\ud83d\udcf1 Bot Client: {} Running\
"
            ).format(
                "\u2705" if self.is_monitoring else "\u274c",
                "\u2705" if self.user_client and self.user_client.is_connected() else "\u274c",
                "\u2705" if self.client.is_connected() else "\u274c"
            )
            
            await event.respond(status_text)

        @self.client.on(events.NewMessage(pattern='/stop'))
        async def stop_handler(event):
            if not self.is_admin(event.sender_id):
                return
            
            logger.info("\ud83d\uded1 Stop command from admin {}".format(event.sender_id))
            
            await self.stop_monitoring()
            await event.respond("\ud83d\uded1 **Monitoring Stopped**\
"
                               "\
"
                               "Bot is no longer monitoring for new messages.")

        @self.client.on(events.NewMessage(pattern='/restart'))
        async def restart_handler(event):
            if not self.is_admin(event.sender_id):
                return
            
            logger.info("\ud83d\udd04 Restart command from admin {}".format(event.sender_id))
            
            await event.respond("\ud83d\udd04 **Restarting...**\
"
                               "\
"
                               "Stopping current monitoring...")
            
            await self.stop_monitoring()
            await asyncio.sleep(2)
            
            if self.session_string:
                success = await self.setup_user_client(self.session_string)
                if success:
                    await event.respond("\u2705 **Restart Successful**\
"
                                       "\
"
                                       "Monitoring is now active again.")
                else:
                    await event.respond("\u274c **Restart Failed**\
"
                                       "\
"
                                       "Unable to reconnect. Please check session string.")
            else:
                await event.respond("\u274c **No Session Available**\
"
                                   "\
"
                                   "Please add a session string first with /addstring")

        @self.client.on(events.NewMessage(pattern='/stats'))
        async def stats_handler(event):
            if not self.is_admin(event.sender_id):
                return
            
            logger.info("\ud83d\udcc8 Stats command from admin {}".format(event.sender_id))
            
            try:
                # Get detailed statistics
                self.cursor.execute('''
                    SELECT DATE(extracted_at) as date, COUNT(*) as count 
                    FROM extracted_cards 
                    WHERE DATE(extracted_at) >= DATE("now", "-7 days")
                    GROUP BY DATE(extracted_at) 
                    ORDER BY date DESC
                ''')
                daily_stats = self.cursor.fetchall()
                
                stats_text = "\ud83d\udcc8 Detailed Statistics\
"
                "\
"
                
                if daily_stats:
                    stats_text += "Last 7 Days:\
"
                    for date, count in daily_stats:
                        stats_text += "\u2022 {}: {} cards\
".format(date, count)
                else:
                    stats_text += "No data available for the last 7 days.\
"
                
                await event.respond(stats_text)
                
            except Exception as e:
                logger.error("Error getting detailed stats: {}".format(e))
                await event.respond("\u274c Error retrieving statistics.")

        @self.client.on(events.NewMessage)
        async def message_handler(event):
            user_id = event.sender_id
            
            # Only process admin messages
            if not self.is_admin(user_id):
                return
            
            message_text = event.message.text
            
            # Skip if it's a command
            if message_text.startswith('/'):
                return
            
            # Handle session string input
            if user_id in self.user_states and self.user_states[user_id]['step'] == 'awaiting_session':
                session_string = message_text.strip()
                
                # Basic validation
                if len(session_string) < 100:
                    await event.respond(
                        "\u274c **Invalid Session String**\
"
                        "\
"
                        "Session strings are typically much longer (200+ characters).\
"
                        "Please make sure you copied the complete session string."
                    )
                    return
                
                await event.respond("\u23f3 **Validating Session...**\
"
                                   "\
"
                                   "This may take a few seconds...")
                
                # Validate session
                if await self.validate_session(session_string):
                    # Save session
                    self.session_string = session_string
                    self.save_session_to_file(session_string)
                    await self.save_session_to_db(session_string)
                    
                    await event.respond("\u2705 **Session Validated**\
"
                                       "\
"
                                       "Setting up monitoring...")
                    
                    # Setup user client
                    success = await self.setup_user_client(session_string)
                    
                    if success:
                        await event.respond(
                            "\ud83d\ude80 **Setup Complete!**\
"
                            "\
"
                            "\u2705 Bot is now monitoring group {}\
"
                            "\
"
                            "\ud83d\udce4 Extracted cards will be sent to {} target channel(s)\
"
                            "\
"
                            "\ud83d\udd0d **Monitoring is ACTIVE!**\
"
                            "\
"
                            "Use /status to check bot status anytime.".format(
                                self.SOURCE_GROUP_ID, len(self.TARGET_CHANNELS)
                            )
                        )
                    else:
                        await event.respond(
                            "\u274c **Setup Failed**\
"
                            "\
"
                            "Session is valid but failed to setup monitoring.\
"
                            "Please try /restart or contact support."
                        )
                else:
                    await event.respond(
                        "\u274c **Invalid Session**\
"
                        "\
"
                        "The session string appears to be invalid or expired.\
"
                        "Please generate a new session string and try again.\
"
                        "\
"
                        "Use /addstring to try again."
                    )
                
                # Clean up state
                if user_id in self.user_states:
                    del self.user_states[user_id]

    async def cleanup(self):
        """Cleanup resources"""
        try:
            logger.info("\ud83e\uddf9 Cleaning up...")
            
            await self.stop_monitoring()
            
            if self.client and self.client.is_connected():
                await self.client.disconnect()
            
            if self.conn:
                self.conn.close()
            
            logger.info("\u2705 Cleanup complete")
            
        except Exception as e:
            logger.error("Error during cleanup: {}".format(e))

# Signal handlers
def signal_handler(signum, frame):
    logger.info("\ud83d\udce1 Received signal {}".format(signum))
    asyncio.create_task(cleanup_and_exit())

async def cleanup_and_exit():
    logger.info("\ud83d\uded1 Shutting down...")
    sys.exit(0)

# Main execution
async def main():
    # Your actual configuration - UPDATE THESE VALUES
    API_ID = 28708347
    API_HASH = "4fa9a7becae9889c9052871a24facfac"
    BOT_TOKEN = "8361809176:AAEhVdiCv6KeQpCenfbelfiEb-8W4Dwf4WY"  # Replace with your actual bot token
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    bot = None
    try:
        logger.info("\ud83d\ude80 Starting xforce.....")
        
        bot = TelegramCardExtractor(API_ID, API_HASH, BOT_TOKEN)
        await bot.start_bot()
        
    except KeyboardInterrupt:
        logger.info("\u2328\ufe0f Bot stopped by user")
    except Exception as e:
        logger.error("\ud83d\udca5 Fatal error: {}".format(e))
    finally:
        if bot:
            await bot.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\ud83d\udc4b Bot stopped by user")
    except Exception as e:
        logger.error("\ud83d\udca5 Fatal error: {}".format(e))
        sys.exit(1)
