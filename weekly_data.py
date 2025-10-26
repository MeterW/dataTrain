import asyncio
import websockets
import json
import sqlite3
from datetime import datetime, timedelta
import time
import os

# Configuration from environment variables
API_TOKEN = os.environ.get('DERIV_API_TOKEN', 'YOUR_API_TOKEN_HERE')
APP_ID = 1089
WS_URL = f'wss://ws.derivws.com/websockets/v3?app_id={APP_ID}'

# Date range options
START_DATE = os.environ.get('START_DATE', '')  # Format: YYYY-MM-DD
END_DATE = os.environ.get('END_DATE', '')      # Format: YYYY-MM-DD
WEEKS_AGO = os.environ.get('WEEKS_AGO', '')    # Alternative: use weeks_ago

class DateRangeTicksFetcher:
    def __init__(self, start_date=None, end_date=None):
        self.ws = None
        
        # Parse dates
        if start_date and end_date:
            # Use explicit date range
            # Start: beginning of start_date (00:00:00)
            self.start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            # End: end of day BEFORE end_date (23:59:59 of previous day)
            # This prevents overlap: end_date is EXCLUSIVE
            self.end_dt = datetime.strptime(end_date, '%Y-%m-%d') - timedelta(seconds=1)
            self.label = f"{start_date}_to_{end_date}"
        else:
            # Use weeks_ago (default behavior)
            weeks_ago = int(WEEKS_AGO) if WEEKS_AGO else 1
            now = datetime.now()
            self.end_dt = now - timedelta(weeks=weeks_ago)
            self.start_dt = self.end_dt - timedelta(days=7)
            self.label = f"week_{weeks_ago}"
        
        self.start_time = int(self.start_dt.timestamp())
        self.end_time = int(self.end_dt.timestamp())
        self.db_path = f'deriv_ticks_{self.label}.sqlite'
        
        print(f"\n{'='*70}")
        print(f"FETCHING TICKS DATA: {self.label}")
        print(f"{'='*70}")
        print(f"Start: {self.start_dt.strftime('%Y-%m-%d %H:%M:%S')} (inclusive)")
        print(f"End:   {self.end_dt.strftime('%Y-%m-%d %H:%M:%S')} (inclusive)")
        print(f"Note:  End date is EXCLUSIVE - data fetched UP TO but NOT including end date")
        print(f"Duration: ~{(self.end_dt - self.start_dt).days} days")
        print(f"Database: {self.db_path}")
        print(f"{'='*70}\n")
        
    async def connect(self):
        """Connect to Deriv WebSocket API"""
        self.ws = await websockets.connect(WS_URL)
        print("✓ Connected to Deriv API")
        
        # Authorize
        await self.ws.send(json.dumps({"authorize": API_TOKEN}))
        response = json.loads(await self.ws.recv())
        
        if response.get('error'):
            raise Exception(f"Authorization failed: {response['error']['message']}")
        
        print(f"✓ Authorized as {response['authorize']['loginid']}\n")
        
    async def get_evenodd_symbols(self):
        """Get symbols that support even/odd"""
        return [
            {'symbol': 'R_10', 'display_name': 'Volatility 10 Index'},
            {'symbol': 'R_25', 'display_name': 'Volatility 25 Index'},
            {'symbol': 'R_50', 'display_name': 'Volatility 50 Index'},
            {'symbol': 'R_75', 'display_name': 'Volatility 75 Index'},
            {'symbol': 'R_100', 'display_name': 'Volatility 100 Index'},
            {'symbol': '1HZ10V', 'display_name': 'Volatility 10 (1s) Index'},
            {'symbol': '1HZ25V', 'display_name': 'Volatility 25 (1s) Index'},
            {'symbol': '1HZ50V', 'display_name': 'Volatility 50 (1s) Index'},
            {'symbol': '1HZ75V', 'display_name': 'Volatility 75 (1s) Index'},
            {'symbol': '1HZ100V', 'display_name': 'Volatility 100 (1s) Index'},
        ]
    
    async def fetch_ticks_for_period(self, symbol):
        """Fetch ticks for a specific time period"""
        print(f"Fetching {symbol}...")
        
        all_ticks = []
        current_end = self.end_time
        pip_size = 2
        batch_count = 0
        
        while current_end > self.start_time:
            batch_count += 1
            
            await self.ws.send(json.dumps({
                "ticks_history": symbol,
                "adjust_start_time": 1,
                "count": 5000,
                "end": current_end,
                "start": self.start_time,
                "style": "ticks"
            }))
            
            try:
                response = json.loads(await self.ws.recv())
            except Exception as e:
                print(f"  ✗ Connection error: {e}")
                break
            
            if response.get('error'):
                print(f"  ✗ Error: {response['error']['message']}")
                break
            
            if 'history' not in response:
                print(f"  ✗ No history data")
                break
            
            history = response['history']
            times = history.get('times', [])
            prices = history.get('prices', [])
            pip_size = response.get('pip_size', 2)
            
            if not times:
                break
            
            # Add ticks within our time range
            for epoch, quote in zip(times, prices):
                if self.start_time <= epoch <= self.end_time:
                    all_ticks.append({
                        'epoch': epoch,
                        'quote': quote,
                        'symbol': symbol,
                        'pip_size': pip_size
                    })
            
            oldest_time = datetime.fromtimestamp(times[0]).strftime('%Y-%m-%d %H:%M:%S')
            print(f"  Batch {batch_count}: {len(times)} ticks (oldest: {oldest_time})")
            
            oldest_epoch = times[0]
            if oldest_epoch <= self.start_time:
                break
            
            current_end = oldest_epoch - 1
            await asyncio.sleep(0.3)
        
        print(f"  ✓ Total: {len(all_ticks)} ticks for {symbol} ({batch_count} batches)\n")
        return all_ticks
    
    def check_existing_data(self):
        """Check what data already exists in the database"""
        if not os.path.exists(self.db_path):
            return None
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT 
                    symbol,
                    MIN(epoch) as oldest_tick,
                    MAX(epoch) as newest_tick,
                    COUNT(*) as tick_count
                FROM ticks
                GROUP BY symbol
            ''')
            
            existing = {}
            for row in cursor.fetchall():
                symbol, oldest, newest, count = row
                existing[symbol] = {
                    'oldest': datetime.fromtimestamp(oldest),
                    'newest': datetime.fromtimestamp(newest),
                    'count': count
                }
            
            conn.close()
            return existing
        except:
            conn.close()
            return None
    
    def setup_database(self):
        """Create database and tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticks (
                epoch INTEGER,
                quote REAL,
                symbol TEXT,
                pip_size INTEGER,
                PRIMARY KEY (epoch, symbol)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                contract_id INTEGER PRIMARY KEY,
                start_time TEXT NOT NULL,
                symbol TEXT NOT NULL,
                contract_type TEXT NOT NULL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        
        cursor.execute('''
            INSERT OR REPLACE INTO metadata (key, value) VALUES 
            ('start_time', ?),
            ('end_time', ?),
            ('fetched_at', ?),
            ('label', ?)
        ''', (
            self.start_dt.isoformat(),
            self.end_dt.isoformat(),
            datetime.now().isoformat(),
            self.label
        ))
        
        conn.commit()
        conn.close()
        print(f"✓ Database ready: {self.db_path}\n")
    
    def save_ticks_to_db(self, ticks):
        """Save ticks to database"""
        if not ticks:
            return 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.executemany('''
            INSERT OR IGNORE INTO ticks (epoch, quote, symbol, pip_size)
            VALUES (:epoch, :quote, :symbol, :pip_size)
        ''', ticks)
        
        conn.commit()
        rows_inserted = cursor.rowcount
        conn.close()
        
        return rows_inserted
    
    async def fetch_all(self):
        """Fetch all ticks for the specified period"""
        # Check existing data
        existing = self.check_existing_data()
        if existing:
            print("⚠️  Database already exists with data:")
            for symbol, info in existing.items():
                print(f"  {symbol}: {info['count']} ticks ({info['oldest']} to {info['newest']})")
            print()
        
        await self.connect()
        self.setup_database()
        
        symbols = await self.get_evenodd_symbols()
        
        total_ticks = 0
        for symbol_info in symbols:
            symbol = symbol_info['symbol']
            ticks = await self.fetch_ticks_for_period(symbol)
            
            if ticks:
                rows = self.save_ticks_to_db(ticks)
                total_ticks += len(ticks)
                print(f"  ✓ Saved {rows} new ticks ({len(ticks) - rows} duplicates skipped)\n")
            
            await asyncio.sleep(0.5)
        
        await self.ws.close()
        
        print(f"\n{'='*70}")
        print(f"✓ COMPLETE!")
        print(f"{'='*70}")
        print(f"Period: {self.start_dt.strftime('%Y-%m-%d')} to {self.end_dt.strftime('%Y-%m-%d')}")
        print(f"Total ticks fetched: {total_ticks}")
        print(f"Database: {self.db_path}")
        print(f"{'='*70}\n")

async def main():
    # Check if using explicit dates or weeks_ago
    if START_DATE and END_DATE:
        fetcher = DateRangeTicksFetcher(start_date=START_DATE, end_date=END_DATE)
    else:
        fetcher = DateRangeTicksFetcher()
    
    await fetcher.fetch_all()

if __name__ == "__main__":
    asyncio.run(main())