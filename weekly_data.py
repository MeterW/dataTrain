import asyncio
import websockets
import json
import sqlite3
from datetime import datetime, timedelta
import time
import os
import sys

# Force unbuffered output for GitHub Actions
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

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
            # Bear/Bull Market Indices
            {'symbol': 'RDBEAR', 'display_name': 'Bear Market Index'},
            {'symbol': 'RDBULL', 'display_name': 'Bull Market Index'},
            
            # Jump Indices
            {'symbol': 'JD10', 'display_name': 'Jump 10 Index'},
            {'symbol': 'JD25', 'display_name': 'Jump 25 Index'},
            {'symbol': 'JD50', 'display_name': 'Jump 50 Index'},
            {'symbol': 'JD75', 'display_name': 'Jump 75 Index'},
            {'symbol': 'JD100', 'display_name': 'Jump 100 Index'},
            
            # Volatility Indices (1s)
            {'symbol': '1HZ10V', 'display_name': 'Volatility 10 (1s) Index'},
            {'symbol': '1HZ15V', 'display_name': 'Volatility 15 (1s) Index'},
            {'symbol': '1HZ25V', 'display_name': 'Volatility 25 (1s) Index'},
            {'symbol': '1HZ30V', 'display_name': 'Volatility 30 (1s) Index'},
            {'symbol': '1HZ50V', 'display_name': 'Volatility 50 (1s) Index'},
            {'symbol': '1HZ75V', 'display_name': 'Volatility 75 (1s) Index'},
            {'symbol': '1HZ90V', 'display_name': 'Volatility 90 (1s) Index'},
            {'symbol': '1HZ100V', 'display_name': 'Volatility 100 (1s) Index'},
            
            # Volatility Indices (standard)
            {'symbol': 'R_10', 'display_name': 'Volatility 10 Index'},
            {'symbol': 'R_25', 'display_name': 'Volatility 25 Index'},
            {'symbol': 'R_50', 'display_name': 'Volatility 50 Index'},
            {'symbol': 'R_75', 'display_name': 'Volatility 75 Index'},
            {'symbol': 'R_100', 'display_name': 'Volatility 100 Index'},
        ]
    
    async def fetch_ticks_for_period(self, symbol):
        """Fetch ticks for a specific time period"""
        start_fetch_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching {symbol}...", flush=True)
        
        all_ticks = []
        current_end = self.end_time
        pip_size = 2
        batch_count = 0
        last_progress_time = time.time()
        
        while current_end > self.start_time:
            batch_count += 1
            
            # Progress update every 30 seconds
            if time.time() - last_progress_time > 30:
                elapsed = time.time() - start_fetch_time
                print(f"  [{symbol}] Still fetching... {batch_count} batches, {len(all_ticks)} ticks, {elapsed:.0f}s elapsed", flush=True)
                last_progress_time = time.time()
            
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
                print(f"  ✗ Connection error: {e}", flush=True)
                break
            
            if response.get('error'):
                print(f"  ✗ Error: {response['error']['message']}", flush=True)
                break
            
            if 'history' not in response:
                print(f"  ✗ No history data", flush=True)
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
            
            # Print progress every 10 batches
            if batch_count % 10 == 0:
                print(f"  [{symbol}] Batch {batch_count}: {len(all_ticks)} total ticks (oldest: {oldest_time})", flush=True)
            
            oldest_epoch = times[0]
            if oldest_epoch <= self.start_time:
                break
            
            current_end = oldest_epoch - 1
            await asyncio.sleep(0.3)
        
        elapsed = time.time() - start_fetch_time
        print(f"  ✓ {symbol}: {len(all_ticks)} ticks in {batch_count} batches ({elapsed:.1f}s)\n", flush=True)
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
        overall_start = time.time()
        
        # Check existing data
        existing = self.check_existing_data()
        if existing:
            print("⚠️  Database already exists with data:", flush=True)
            for symbol, info in existing.items():
                print(f"  {symbol}: {info['count']} ticks ({info['oldest']} to {info['newest']})", flush=True)
            print(flush=True)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Connecting to Deriv API...", flush=True)
        await self.connect()
        self.setup_database()
        
        symbols = await self.get_evenodd_symbols()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting fetch for {len(symbols)} symbols\n", flush=True)
        
        total_ticks = 0
        for idx, symbol_info in enumerate(symbols, 1):
            symbol = symbol_info['symbol']
            print(f"[{idx}/{len(symbols)}] Processing {symbol}...", flush=True)
            
            ticks = await self.fetch_ticks_for_period(symbol)
            
            if ticks:
                rows = self.save_ticks_to_db(ticks)
                total_ticks += len(ticks)
                print(f"  ✓ Saved {rows} new ticks ({len(ticks) - rows} duplicates skipped)", flush=True)
                
                # Show overall progress
                elapsed = time.time() - overall_start
                avg_time_per_symbol = elapsed / idx
                remaining_symbols = len(symbols) - idx
                eta_seconds = avg_time_per_symbol * remaining_symbols
                eta_minutes = eta_seconds / 60
                
                print(f"  Progress: {idx}/{len(symbols)} symbols | Total ticks: {total_ticks:,} | ETA: {eta_minutes:.1f}m\n", flush=True)
            
            await asyncio.sleep(0.5)
        
        await self.ws.close()
        
        elapsed_total = time.time() - overall_start
        print(f"\n{'='*70}", flush=True)
        print(f"✓ COMPLETE!", flush=True)
        print(f"{'='*70}", flush=True)
        print(f"Period: {self.start_dt.strftime('%Y-%m-%d')} to {self.end_dt.strftime('%Y-%m-%d')}", flush=True)
        print(f"Total ticks fetched: {total_ticks:,}", flush=True)
        print(f"Time taken: {elapsed_total/60:.1f} minutes", flush=True)
        print(f"Database: {self.db_path}", flush=True)
        print(f"{'='*70}\n", flush=True)

async def main():
    # Check if using explicit dates or weeks_ago
    if START_DATE and END_DATE:
        fetcher = DateRangeTicksFetcher(start_date=START_DATE, end_date=END_DATE)
    else:
        fetcher = DateRangeTicksFetcher()
    
    await fetcher.fetch_all()

if __name__ == "__main__":
    asyncio.run(main())
