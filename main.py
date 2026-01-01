from nicegui import ui, app, run
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import os
import sqlite3
import re
import collections
import io
import nltk
import asyncio
import numpy as np
import random
import math
import json
import urllib.parse
import time
import dateutil.parser
import html
from textblob import TextBlob
from collections import Counter

# --- GLOBAL LOCK ---
auto_pilot_lock = asyncio.Lock()

# --- API KEYS ---
MAX_RESULTS_RAPID = 100
MAX_RESULTS_OFFICIAL = 20

# Official X API Bearer Tokens
# Load from environment variable as a JSON list, e.g., '["token1", "token2"]'
BEARER_TOKENS = json.loads(os.getenv("BEARER_TOKENS", "[]"))

# Rapid API Keys
# Load from environment variable as a JSON list
RAPID_API_KEYS = json.loads(os.getenv("RAPID_API_KEYS", "[]"))

# --- Custom CSS ---
CUSTOM_CSS = """
<style>
    :root {
        --bg: #f8fafc;
        --bg-gradient: linear-gradient(135deg, rgba(205, 161, 105, 0.1) 0%, #ffffff 45%, #ffffff 55%, rgba(131, 212, 117, 0.1) 100%);
        --text-primary: #1e293b;
        --text-secondary: #64748b;
        --accent: #2563eb;
        --accent-soft: rgba(37, 99, 235, 0.1);
        --card-bg: #ffffff;
        --card-border: #e2e8f0;
        --card-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
        --sidebar-bg: #ffffff;
        --sidebar-border: #eff3f4;
    }

    body.body--dark {
        --bg: #000000;
        --bg-gradient: #000000;
        --text-primary: #e7e9ea;
        --text-secondary: #71767b;
        --accent: #1d9bf0;
        --accent-soft: rgba(29, 155, 240, 0.1);
        --card-bg: #000000;
        --card-border: #2f3336;
        --card-shadow: none;
        --sidebar-bg: #000000;
        --sidebar-border: #2f3336;
    }

    body {
        background-color: var(--bg);
        background-image: var(--bg-gradient);
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
        color: var(--text-primary);
    }

    /* X-Style Sidebar */
    .custom-sidebar {
        background: var(--sidebar-bg);
        border-right: 1px solid var(--sidebar-border);
    }

    .nav-btn {
        font-size: 1.25rem;
        padding: 12px 24px;
        border-radius: 9999px;
        color: var(--text-primary);
        transition: background-color 0.2s;
        justify-content: flex-start;
        text-transform: none;
        font-weight: 500;
        margin-bottom: 8px;
    }
    .nav-btn.active {
        font-weight: 800;
        color: var(--accent);
    }
    .nav-btn:hover {
        background-color: var(--accent-soft);
        color: var(--accent);
    }

    /* Tweet Cards */
    .tweet-container {
        background: var(--card-bg);
        border: 1px solid var(--card-border);
        border-radius: 16px;
        padding: 16px;
        margin-bottom: 16px;
        color: var(--text-primary);
    }
    
    .tweet-container:hover {
        background-color: rgba(0,0,0,0.01);
    }
    
    body.body--dark .tweet-container:hover {
        background-color: rgba(255,255,255,0.03);
    }

    .new-tweet-highlight {
        border-left: 4px solid var(--accent);
    }

    /* Profile Pic */
    .profile-pic {
        width: 48px;
        height: 48px;
        border-radius: 50%;
        object-fit: cover;
    }

    /* Media Grid */
    .media-grid {
        display: grid;
        gap: 2px;
        border-radius: 16px;
        overflow: hidden;
        border: 1px solid var(--card-border);
        margin-top: 12px;
    }
    .media-item {
        width: 100%;
        height: 100%;
        object-fit: cover;
        cursor: pointer;
    }
    
    /* Loading Overlay */
    .loading-spinner { animation: spin 1s linear infinite; }
    @keyframes spin { 100% { transform: rotate(360deg); } }
    
    /* Pulse Animation */
    .pulse-container {
        height: 3px;
        background: var(--accent-soft);
        border-radius: 2px;
        overflow: hidden;
        margin-bottom: 15px;
    }
    .pulse-particle {
        width: 30%;
        height: 100%;
        background: var(--accent);
        animation: pulse 2s infinite ease-in-out;
    }
    @keyframes pulse {
        0% { transform: translateX(-100%); }
        100% { transform: translateX(400%); }
    }
    
    /* Skeleton Loader */
    .skeleton {
        background: linear-gradient(90deg, #f0f0f0 25%, #e0e0e0 50%, #f0f0f0 75%);
        background-size: 200% 100%;
        animation: shimmer 1.5s infinite;
        border-radius: 8px;
    }
    .body--dark .skeleton {
        background: linear-gradient(90deg, #16181c 25%, #2f3336 50%, #16181c 75%);
    }
    @keyframes shimmer {
        0% { background-position: 200% 0; }
        100% { background-position: -200% 0; }
    }

    /* Chart Containers */
    .chart-container, .premium-metric-card, .alert-log-container {
        background: var(--card-bg);
        border: 1px solid var(--card-border);
        border-radius: 16px;
        padding: 16px;
    }

    /* Alert Items */
    .alert-item {
        border-bottom: 1px solid rgba(150, 150, 150, 0.1);
        padding: 8px 0;
    }
    .alert-item:last-child { border-bottom: none; }

    /* Text Colors */
    .text-blue-900 { color: var(--accent) !important; }
    .text-gray-500, .text-gray-600 { color: var(--text-secondary) !important; }
    
    /* Sentiment Colors */
    .text-green-600 { color: #00ba7c !important; }
    .text-red-600 { color: #f91880 !important; }
    .text-green-500 { color: #00ba7c !important; }
    .text-red-500 { color: #f91880 !important; }
    
    /* 3D Logo Effect */
    .logo-3d {
        transition: transform 0.5s ease;
        transform: perspective(500px) rotateY(15deg);
        filter: drop-shadow(4px 4px 6px rgba(0,0,0,0.3));
    }
    .logo-3d:hover {
        transform: perspective(500px) rotateY(0deg) scale(1.05);
        filter: drop-shadow(2px 2px 4px rgba(0,0,0,0.4));
    }
    
    /* Standard Inputs */
    .q-field__control {
        border-radius: 9999px !important;
    }
    
    /* Right Sidebar Styling */
    .right-sidebar {
        border-left: 1px solid var(--sidebar-border);
        /* Scrollbar hiding for cleaner look but functional */
        scrollbar-width: thin;
        scrollbar-color: var(--accent-soft) transparent;
    }
    
    /* Normal Logo (No 3D) */
    .logo-normal {
        filter: drop-shadow(0 2px 4px rgba(0,0,0,0.1));
        transition: transform 0.2s;
    }
    .logo-normal:hover {
        transform: scale(1.05);
    }
</style>
"""

# --- CONFIG & CONSTANTS ---
DB_PATH = "tweet_monitor.db"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
KEYWORD = '(CISF OR Cisf OR cisf OR सीआईएसएफ OR "सेंट्रल इंडस्ट्रियल सिक्योरिटी फोर्स" OR "Central industrial security force" OR @cisfhqrs OR @CISFAirport)'

# --- Database Functions ---
def db_connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def db_init():
    with db_connect() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tweets (
                id TEXT PRIMARY KEY, datetime TEXT, author_id TEXT, author_username TEXT,
                content TEXT, url TEXT, sentiment TEXT, query_topic TEXT, first_seen TEXT
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_datetime ON tweets(datetime);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_topic ON tweets(query_topic);")
        cursor.execute("CREATE TABLE IF NOT EXISTS blocked_users (author_id TEXT PRIMARY KEY);")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT, token TEXT UNIQUE,
                usage_count INTEGER DEFAULT 0, failure_count INTEGER DEFAULT 0, last_used TEXT
            );
        """)
        
        # Check and add columns dynamically
        cursor.execute("PRAGMA table_info(tweets)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        new_columns = {
            "followers": "INTEGER", "retweets": "INTEGER", "replies": "INTEGER",
            "likes": "INTEGER", "views": "INTEGER", "author_pfp_url": "TEXT",
            "author_name": "TEXT", "category": "TEXT", "priority": "INTEGER", 
            "summary": "TEXT", "is_important": "INTEGER", "source": "TEXT", 
            "is_verified": "INTEGER",
            "media_info": "TEXT" # NEW COLUMN FOR JSON MEDIA DATA
        }
        for col_name, col_type in new_columns.items():
            if col_name not in existing_columns:
                print(f"Adding column {col_name} to tweets table")
                cursor.execute(f"ALTER TABLE tweets ADD COLUMN {col_name} {col_type} DEFAULT NULL")
        conn.commit()

def db_upsert_tweets(tweets, query_topic):
    new_ids = []
    with db_connect() as conn:
        cursor = conn.cursor()
        now_str = datetime.now().isoformat()
        for tweet in tweets:
            try:
                # Convert list of media dicts to JSON string
                media_json = json.dumps(tweet.get('media_files', []))
                
                cursor.execute("""
                    INSERT INTO tweets (
                        id, datetime, author_id, author_username, author_name, content, url, 
                        sentiment, query_topic, first_seen, followers, retweets, replies, likes, 
                        views, author_pfp_url, category, priority, summary, is_important, 
                        source, is_verified, media_info
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                    ON CONFLICT(id) DO NOTHING;
                """, (
                    tweet['id'], tweet['datetime'], tweet['author_id'], tweet['author_username'], tweet.get('author_name', 'Unknown'),
                    tweet['content'], tweet['url'], tweet['sentiment'], query_topic, now_str,
                    tweet['followers'], tweet['retweets'], tweet['replies'], tweet['likes'], tweet['views'],
                    tweet['author_pfp_url'],
                    tweet.get('category', 'General'), tweet.get('priority', 1), tweet.get('summary', ''), tweet.get('is_important', 0),
                    tweet.get('source', 'Unknown'), tweet.get('is_verified', 0),
                    media_json
                ))
                if cursor.rowcount > 0:
                    new_ids.append(tweet['id'])
            except Exception as e:
                print(f"DB Error inserting tweet {tweet['id']}: {e}")
        conn.commit()
    return new_ids

def db_fetch_tweets(query_topic):
    with db_connect() as conn:
        df = pd.read_sql_query("SELECT * FROM tweets WHERE query_topic = ?", conn, params=(query_topic,))
    return df

def db_get_most_recent_id(query_topic):
    with db_connect() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(id) FROM tweets WHERE query_topic = ?", (query_topic,))
        result = cursor.fetchone()[0]
    return result

def db_get_blocked_users():
    with db_connect() as conn:
        ids = [row[0] for row in conn.execute("SELECT author_id FROM blocked_users").fetchall()]
    return ids

def db_add_blocked_users(author_ids):
    with db_connect() as conn:
        for author_id in author_ids:
            if author_id:
                conn.execute("INSERT OR IGNORE INTO blocked_users (author_id) VALUES (?)", (author_id.strip(),))
        conn.commit()

def db_remove_blocked_user(author_id):
    with db_connect() as conn:
        conn.execute("DELETE FROM blocked_users WHERE author_id = ?", (author_id,))
        conn.commit()

db_init()

# --- Analytics Functions (TextBlob ONLY) ---
def get_sentiment(text):
    """
    Analyzes sentiment using TextBlob (completely offline/local).
    Returns 'Positive', 'Negative', or 'Neutral'.
    """
    if not isinstance(text, str) or not text.strip(): 
        return "Neutral"
    
    # TextBlob analysis
    analysis = TextBlob(text)
    polarity = analysis.sentiment.polarity
    
    if polarity > 0.05: return 'Positive'
    elif polarity < -0.05: return 'Negative'
    else: return 'Neutral'

def clean_tweet_content(text):
    return re.sub(r'https?://t\.co/\w+', '', text).strip()

def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram ENV not set, skipping message")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "disable_web_page_preview": True
        }
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Telegram Send Error: {e}")

# --- Helper Functions ---
def extract_issue(text):
    issues_map = {
        "Security Check": ["frisking", "check", "security", "belt", "tray", "scan"],
        "Queue/Delay": ["queue", "line", "wait", "long", "slow", "delay", "crowd", "rush"],
        "Staff Behavior": ["rude", "behavior", "staff", "argument", "shout", "polite", "misbehave"],
        "Lost Item": ["lost", "missing", "baggage", "bag", "theft", "stolen", "left behind"],
        "Facility": ["water", "washroom", "toilet", "clean", "dirty", "seat", "ac", "hot"]
    }
    for issue, keywords in issues_map.items():
        if any(k in text.lower() for k in keywords):
            return issue
    return "General"

def extract_hashtags(text):
    # Decode HTML entities first (e.g. &#x624;)
    text = html.unescape(text)
    # Find hashtags
    tags = re.findall(r'#(\w+)', text)
    # Filter out garbage tags (e.g., hex codes like x624, or single characters)
    clean_tags = []
    for t in tags:
        # Ignore if it starts with 'x' followed by digits (common hex artifact)
        if re.match(r'^x\d+$', t): continue
        if len(t) > 2:
            clean_tags.append(t)
    return clean_tags

def to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

def detect_anomalies(df):
    if df.empty or len(df) < 10: return None
    if not pd.api.types.is_datetime64_any_dtype(df['datetime_dt']): return None
    now = datetime.now(timezone.utc)
    recent_df = df[df['datetime_dt'] >= now - timedelta(hours=1)]
    time_span_hours = (df['datetime_dt'].max() - df['datetime_dt'].min()).total_seconds() / 3600
    if time_span_hours < 1: time_span_hours = 1
    historical_avg_vol = len(df) / time_span_hours
    if historical_avg_vol > 0.1 and len(recent_df) > historical_avg_vol * 2.5:
        return f"🚨 **Spike Alert:** {len(recent_df)} tweets/hr vs avg {historical_avg_vol:.1f}"
    if not recent_df.empty:
        recent_neg = len(recent_df[recent_df['sentiment'] == 'Negative'])
        recent_neg_percent = (recent_neg / len(recent_df)) * 100
        hist_neg = len(df[df['sentiment'] == 'Negative'])
        hist_neg_percent = (hist_neg / len(df)) * 100
        if recent_neg_percent > max(hist_neg_percent * 2.0, 25):
             return f"🚨 **Sentiment Alert:** Negative sentiment is {recent_neg_percent:.0f}% (High)"
    return None

def normalize_timestamp(date_input):
    if not date_input: return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        if isinstance(date_input, datetime): dt = date_input
        else: dt = dateutil.parser.parse(str(date_input))
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        else: dt = dt.astimezone(timezone.utc)
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    except: return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

# --- API FETCHING LOGIC ---
token_state = {'token_index': 0, 'rapid_token_index': 0}

def get_tweets_rapidapi(keyword):
    current_key_idx = token_state['rapid_token_index']
    
    # Safely get key, fallback to empty string if list is empty
    if not RAPID_API_KEYS:
        return []
        
    current_key = RAPID_API_KEYS[current_key_idx]
    token_state['rapid_token_index'] = (current_key_idx + 1) % len(RAPID_API_KEYS)
    
    safe_keyword = urllib.parse.quote(keyword)
    url = f"https://twitter-aio.p.rapidapi.com/search/{safe_keyword}"
    querystring = {"count": str(MAX_RESULTS_RAPID), "category": "Latest", "filters": "{\"since\": \"2020-10-01\"}", "includeTimestamp": "true"}
    headers = {"x-rapidapi-key": current_key, "x-rapidapi-host": "twitter-aio.p.rapidapi.com"}

    try:
        response = requests.get(url, headers=headers, params=querystring, timeout=30)
        if response.status_code != 200: 
            return []
        
        result = response.json()
        raw = []
        if isinstance(result, list): raw = result
        elif "results" in result: raw = result["results"]
        elif "tweets" in result: raw = result["tweets"]
        elif "entries" in result:
             for entry in result.get("entries", []):
                if entry.get("type") == "TimelineAddEntries":
                    for item in entry.get("entries", []):
                        c = item.get("content", {})
                        if c.get("entryType") == "TimelineTimelineItem":
                            t = c.get("itemContent", {}).get("tweet_results", {}).get("result", {})
                            if t: raw.append(t)

        normalized = []
        for t in raw:
            try:
                tid = t.get("rest_id") or t.get("id") or t.get("id_str")
                if not tid: continue
                tid = str(tid)
                
                # --- Media Extraction for RapidAPI (nested hell) ---
                media_files = []
                legacy = t.get("legacy", {}) if "legacy" in t else t
                
                # Check extended_entities first (standard location for multiple media)
                entities_source = legacy.get("extended_entities") or legacy.get("entities") or t.get("extended_entities") or t.get("entities")
                
                if entities_source and "media" in entities_source:
                    for m in entities_source["media"]:
                        m_type = m.get("type")
                        if m_type == "photo":
                            media_files.append({"type": "image", "url": m.get("media_url_https")})
                        elif m_type == "video" or m_type == "animated_gif":
                            # Find best video variant (highest bitrate mp4)
                            variants = m.get("video_info", {}).get("variants", [])
                            mp4_variants = [v for v in variants if v.get("content_type") == "video/mp4"]
                            if mp4_variants:
                                best_v = max(mp4_variants, key=lambda x: x.get("bitrate", 0))
                                media_files.append({"type": "video", "url": best_v["url"], "preview": m.get("media_url_https")})

                # Basic User & Content Extraction
                if "core" in t and "legacy" in t:
                    user_main = t["core"]["user_results"]["result"]
                    user_legacy = user_main["legacy"]
                    username = user_legacy.get("screen_name", "unknown")
                    name = user_legacy.get("name", "Unknown")
                    author_id = user_main.get("rest_id", "0")
                    pfp = user_legacy.get("profile_image_url_https", "")
                    followers = user_legacy.get("followers_count", 0)
                    is_verified = user_main.get("is_blue_verified", False) or user_legacy.get("verified", False)
                    
                    content = legacy.get("full_text") or legacy.get("text", "")
                    created_at = legacy.get("created_at")
                    retweets = legacy.get("retweet_count", 0)
                    replies = legacy.get("reply_count", 0)
                    likes = legacy.get("favorite_count", 0)
                    views = legacy.get("views", {}).get("count", 0) if isinstance(legacy.get("views"), dict) else 0
                else:
                    user = t.get("user", {})
                    username = user.get("screen_name") or user.get("username", "unknown")
                    name = user.get("name", "Unknown")
                    author_id = user.get("id", "0")
                    pfp = user.get("profile_image_url_https", "")
                    followers = user.get("followers_count", 0)
                    is_verified = user.get("verified", False)
                    content = t.get("text", "")
                    created_at = t.get("created_at")
                    retweets = t.get("retweet_count", 0)
                    replies = t.get("reply_count", 0)
                    likes = t.get("favorite_count", 0)
                    views = t.get("views", 0)

                normalized.append({
                    "id": tid,
                    "datetime": normalize_timestamp(created_at),
                    "author_id": str(author_id),
                    "author_username": username,
                    "author_name": name, 
                    "author_pfp_url": pfp,
                    "content": clean_tweet_content(content),
                    "url": f"https://twitter.com/{username}/status/{tid}",
                    "sentiment": get_sentiment(content),
                    "followers": followers,
                    "retweets": retweets,
                    "replies": replies,
                    "likes": likes,
                    "views": views,
                    "source": "RapidAPI",
                    "is_verified": 1 if is_verified else 0,
                    "media_files": media_files # List of attached media
                })
            except Exception as e:
                continue
        return normalized
    except Exception as e:
        return []

def get_tweets_official_x(bearer_tokens, keyword, since_id=None):
    if not bearer_tokens: return []
    url = "https://api.twitter.com/2/tweets/search/recent"
    # Added attachments and media fields to query
    params = {
        "query": f"{keyword} -is:retweet", "max_results": str(MAX_RESULTS_OFFICIAL),
        "tweet.fields": "created_at,author_id,text,public_metrics,attachments",
        "expansions": "author_id,attachments.media_keys", 
        "user.fields": "username,name,public_metrics,profile_image_url,verified",
        "media.fields": "url,preview_image_url,type,variants" 
    }
    if since_id: params["since_id"] = since_id

    start_index = token_state['token_index']
    for i in range(len(bearer_tokens)):
        current_index = (start_index + i) % len(bearer_tokens)
        token = bearer_tokens[current_index]
        try:
            response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params, timeout=10)
            if response.status_code == 200:
                token_state['token_index'] = (current_index + 1) % len(bearer_tokens)
                data = response.json()
                
                users_data = {u['id']: u for u in data.get('includes', {}).get('users', [])}
                # Create a lookup map for media objects using media_key
                media_data = {m['media_key']: m for m in data.get('includes', {}).get('media', [])}
                
                normalized_tweets = []
                for tweet in data.get("data", []):
                    u = users_data.get(tweet['author_id'], {})
                    m = tweet.get('public_metrics', {})
                    is_verified = u.get('verified', False)

                    # Match media keys to media objects
                    media_files = []
                    if 'attachments' in tweet and 'media_keys' in tweet['attachments']:
                        for mk in tweet['attachments']['media_keys']:
                            media_obj = media_data.get(mk)
                            if media_obj:
                                if media_obj['type'] == 'photo' and 'url' in media_obj:
                                    media_files.append({"type": "image", "url": media_obj['url']})
                                elif media_obj['type'] == 'video' and 'variants' in media_obj:
                                    # Find best MP4 variant
                                    variants = [v for v in media_obj.get('variants', []) if v.get('content_type') == 'video/mp4']
                                    if variants:
                                        best_v = max(variants, key=lambda x: x.get('bitrate', 0))
                                        media_files.append({"type": "video", "url": best_v['url'], "preview": media_obj.get('preview_image_url')})

                    normalized_tweets.append({
                        "id": tweet['id'],
                        "datetime": normalize_timestamp(tweet['created_at']),
                        "author_id": tweet['author_id'],
                        "author_username": u.get('username', "unknown"),
                        "author_name": u.get('name', "Unknown"),
                        "author_pfp_url": u.get('profile_image_url', ''),
                        "content": clean_tweet_content(tweet['text']),
                        "url": f"https://twitter.com/{u.get('username', 'unknown')}/status/{tweet['id']}",
                        "sentiment": get_sentiment(tweet['text']),
                        "followers": u.get('public_metrics', {}).get('followers_count', 0),
                        "retweets": m.get('retweet_count', 0),
                        "replies": m.get('reply_count', 0),
                        "likes": m.get('like_count', 0),
                        "views": m.get('impression_count', 0),
                        "source": "Official X API",
                        "is_verified": 1 if is_verified else 0,
                        "media_files": media_files
                    })
                return normalized_tweets
            elif response.status_code in [401, 429]: continue
        except: return []
    return []

# --- MAIN UI PAGE ---
@ui.page('/')
def main_page():
    ui.add_head_html(CUSTOM_CSS)

    # --- Client State ---
    state = {
        'new_tweet_ids': set(),
        'query': KEYWORD,
        'is_loading': False,
        'timer_label': None,
        'autopilot': False,
        'use_x_api': False, # Default OFF (Use RapidAPI)
        'current_view': 'Feed'
    }
    
    ar_state = {'active': False, 'seconds': 40}
    dark = ui.dark_mode() # Instantiate dark mode

    # --- Loading Overlay ---
    @ui.refreshable
    def loading_overlay():
        if state['is_loading']:
            with ui.element('div').classes('fixed inset-0 bg-black/50 z-[9999] flex flex-col items-center justify-center backdrop-blur-sm'):
                ui.spinner(size='4rem', color='white', thickness=5).classes('loading-spinner')
                source_text = "Official X API" if state['use_x_api'] else "RapidAPI"
                ui.label(f'Fetching via {source_text}...').classes('text-white font-bold mt-6 text-2xl')

    # --- Logic Helpers ---
    async def handle_refresh():
        if auto_pilot_lock.locked(): return
        async with auto_pilot_lock:
            if state['is_loading']: return
            state['is_loading'] = True
            loading_overlay.refresh()
            ui.notify(f"Fetching latest tweets for '{state['query']}'...", color='info')
            
            try:
                processed = []
                if state['use_x_api']:
                    since_id = db_get_most_recent_id(state['query'])
                    processed = await run.io_bound(get_tweets_official_x, BEARER_TOKENS, state['query'], since_id)
                else:
                    processed = await run.io_bound(get_tweets_rapidapi, state['query'])
                
                if not processed:
                    ui.notify("No new tweets found.", color='info')
                    main_layout.refresh()
                    right_sidebar_content.refresh()
                    return

                blocked_ids = set(db_get_blocked_users())
                filtered_tweets = [t for t in processed if t['author_id'] not in blocked_ids]
                filtered_count = len(processed) - len(filtered_tweets)

                newly_inserted_ids = []
                if filtered_tweets:
                    newly_inserted_ids = await run.io_bound(db_upsert_tweets, filtered_tweets, state['query'])
                    if newly_inserted_ids:
                        state['new_tweet_ids'].update(newly_inserted_ids)
                        ui.notify(f"Archived {len(newly_inserted_ids)} new tweets.", color='positive')
                    
                    if state['autopilot'] and newly_inserted_ids:
                        ar_state['active'] = False
                        tweets_to_send = [t for t in filtered_tweets if t['id'] in newly_inserted_ids]
                        try: tweets_to_send.sort(key=lambda x: x['datetime'])
                        except: pass

                        for t in tweets_to_send:
                            try:
                                dt = pd.to_datetime(t['datetime'])
                                if dt.tzinfo is None: dt = dt.tz_localize('UTC')
                                dt_kolkata = dt.tz_convert('Asia/Kolkata')
                                d_date = dt_kolkata.strftime('%d %b, %I:%M %p')
                            except: d_date = str(t['datetime'])
                            
                            a_name = t.get('author_name') or t.get('author_username')
                            media_text = ""
                            if t.get('media_files'):
                                media_text = f"\n[Media Attached: {len(t['media_files'])}]"
                                
                            msg = f"{t['content']}{media_text}\n\n{d_date}\n{a_name} @{t['author_username']}\n{t['url']}"
                            await run.io_bound(send_telegram_message, msg)
                            await asyncio.sleep(1.2)
                        
                        if state['autopilot']: ar_state['active'] = True
                
                if filtered_count > 0: ui.notify(f"Ignored {filtered_count} blocked users.", color='warning')
                main_layout.refresh()
                right_sidebar_content.refresh()
                
            except Exception as e:
                print(f"Error: {e}")
                ui.notify(f"Error: {e}", color='negative')
            finally:
                state['is_loading'] = False
                loading_overlay.refresh()
                ar_state['seconds'] = 40

    async def handle_quick_update():
        ui.notify("Quick Update: Checking for new tweets...", color='primary')
        await handle_refresh()

    def handle_block_submit():
        ids = [id.strip() for id in block_input.value.split(',') if id.strip()]
        if ids:
            db_add_blocked_users(ids)
            ui.notify("User(s) added to blocklist.", color='positive')
            block_input.value = ""
            blocked_list.refresh()
            main_layout.refresh()

    def toggle_autopilot(value):
        state['autopilot'] = value
        ar_state['active'] = value
        content_area.refresh()
        status = "ENABLED" if value else "DISABLED"
        ui.notify(f"Auto Pilot {status}", color='positive' if value else 'warning', icon='rocket')

    def set_view(view_name):
        state['current_view'] = view_name
        main_layout.refresh()

    async def auto_refresh_tick():
        if not ar_state['active'] or state['is_loading']: return
        ar_state['seconds'] -= 1
        if state['timer_label']: state['timer_label'].set_text(f"{ar_state['seconds']}s")
        if ar_state['seconds'] <= 0:
            ar_state['seconds'] = 40
            await handle_refresh()
    
    ui.timer(1.0, auto_refresh_tick)
    
    # --- SHARED CARD RENDERER ---
    def render_tweet_card(row, is_new=False):
        card_classes = 'tweet-container w-full relative'
        if is_new: card_classes += ' new-tweet-highlight'

        # Sentiment Color Logic (QUASAR COLORS)
        sentiment = row.get('sentiment', 'Neutral')
        sentiment_color = {'Positive': 'text-green', 'Negative': 'text-red', 'Neutral': 'text-grey'}.get(sentiment, 'text-grey')

        with ui.card().classes(card_classes):
            # Header
            with ui.row().classes('w-full justify-between items-start mb-2'):
                with ui.row().classes('items-center'):
                    if row['author_pfp_url']: ui.image(row['author_pfp_url']).classes('profile-pic mr-3')
                    else: ui.icon('person', size='md').classes('mr-3 text-gray-400')
                    
                    with ui.column().classes('gap-0'):
                        with ui.row().classes('items-center gap-1'):
                            ui.label(row['author_name']).classes('font-bold text-md leading-none')
                            if row.get('is_verified', 0): ui.icon('verified', size='xs').classes('text-blue-500')
                        
                        with ui.row().classes('items-center gap-2 mt-1'):
                            ui.label(f"@{row['author_username']}").classes('text-sm font-medium text-gray-500')
                            ui.label('•').classes('text-gray-400')
                            try: d_date = row['display_date'].strftime('%d %b, %H:%M')
                            except: d_date = str(row['datetime'])
                            ui.label(d_date).classes('text-xs text-gray-500')
                            # SENTIMENT BALL (Request 3 - using Quasar color)
                            ui.icon('circle', size='10px').classes(sentiment_color).props('flat')
            
            # Content
            safe_content = row['content'].replace('\n#', '\n\\#').replace(' #', ' \\#')
            if safe_content.startswith('#'): safe_content = '\\' + safe_content
            ui.markdown(safe_content).classes('w-full mt-1 mb-2 text-base leading-snug')

            # --- MEDIA RENDERING (Images & Videos) ---
            media_json = row.get('media_info')
            if media_json:
                try:
                    media_files = json.loads(media_json)
                    if media_files:
                        with ui.element('div').classes('w-full mt-2'):
                            # Filter types
                            videos = [m for m in media_files if m.get('type') == 'video']
                            images = [m for m in media_files if m.get('type') == 'image']

                            # Render Video (Full Width)
                            for video in videos:
                                # HTML5 Video Player
                                ui.html(f'''
                                    <video controls class="w-full rounded-xl border border-gray-200" poster="{video.get('preview', '')}">
                                        <source src="{video.get('url')}" type="video/mp4">
                                        Your browser does not support the video tag.
                                    </video>
                                ''')

                            # Render Images (Grid Layout)
                            if images:
                                count = len(images)
                                grid_cols = 1 if count == 1 else 2
                                with ui.element('div').classes('media-grid').style(f'grid-template-columns: repeat({grid_cols}, 1fr);'):
                                    for img in images:
                                        ui.image(img.get('url')).classes('media-item').props('no-spinner')
                                        
                except Exception as e:
                    print(f"Media render error: {e}")

            # Footer Actions
            with ui.row().classes('w-full justify-between items-center mt-3 pt-2 border-t border-gray-100'):
                # Added items-center and moved open_in_new button here
                with ui.row().classes('gap-4 text-gray-500 text-sm items-center'):
                    ui.label(f"👁 {row.get('views', 0):,}")
                    ui.label(f"♥ {row.get('likes', 0):,}")
                    ui.label(f"🔁 {row.get('retweets', 0):,}")
                    # 1. Added Followers
                    ui.label(f"👥 {row.get('followers', 0):,}")
                    # 2. Moved Open Tweet Button here
                    ui.button(icon='open_in_new', on_click=lambda _, url=row['url']: ui.run_javascript(f"window.open('{url}', '_blank')")).props('flat dense size=sm round color=grey')
                
                with ui.row().classes('gap-2'):
                    # Only Block button remains here
                    def quick_block(uid=row['author_id']):
                        db_add_blocked_users([uid])
                        ui.notify(f"Blocked {uid}", color='positive')
                        blocked_list.refresh()
                        main_layout.refresh()
                    ui.button(icon='block', on_click=quick_block).props('flat dense size=sm round color=negative')

    # --- UI Components ---
    @ui.refreshable
    def blocked_list():
        users = db_get_blocked_users()
        with ui.column().classes('w-full'):
            for uid in users:
                with ui.row().classes('items-center w-full justify-between'):
                    ui.label(uid).classes('text-xs text-gray-700')
                    def delete_block(u=uid):
                        db_remove_blocked_user(u)
                        blocked_list.refresh()
                        main_layout.refresh()
                    ui.button(icon='close', on_click=delete_block).props('flat dense size=sm color=red')

    @ui.refreshable
    def right_sidebar_content():
        # Sticky Right Sidebar Content - SCROLLABLE if too tall
        with ui.column().classes('w-80 sticky top-4 max-h-[calc(100vh-2rem)] overflow-y-auto gap-2 right-sidebar pl-4 pr-1'):
            
            # Fetch Data Once for Sidebar
            df = db_fetch_tweets(state['query'])
            recent_df = pd.DataFrame()
            if not df.empty:
                df['datetime_dt'] = pd.to_datetime(df['datetime'], errors='coerce', utc=True)
                now_24h = datetime.now(timezone.utc) - timedelta(hours=24)
                recent_df = df[df['datetime_dt'] >= now_24h]

            # 1. Trending Hashtags (Top)
            with ui.column().classes('bg-gray-50 rounded-xl p-3 w-full'):
                ui.label("Trending Hashtags (24h)").classes('font-bold text-lg mb-2')
                
                if not recent_df.empty:
                    all_text = " ".join(recent_df['content'].tolist())
                    hashtags = extract_hashtags(all_text)
                    top_tags = Counter(hashtags).most_common(5)
                    
                    if top_tags:
                        for tag, count in top_tags:
                            with ui.column().classes('mb-0 cursor-pointer hover:bg-gray-100 p-1 rounded gap-0'):
                                ui.label(f"#{tag}").classes('font-bold text-sm text-blue-900')
                                ui.label(f"{count} posts").classes('text-xs text-gray-500')
                    else:
                        ui.label("No trending hashtags.").classes('text-gray-500 text-sm')
                else:
                    ui.label("No data available.").classes('text-gray-500 text-sm')

            # 2. Sentiment Analytics (Bottom)
            with ui.column().classes('bg-gray-50 rounded-xl p-3 w-full'):
                ui.label("Sentiment (24h)").classes('font-bold text-lg mb-2')
                
                if not recent_df.empty:
                    counts = recent_df['sentiment'].value_counts()
                    total = len(recent_df)
                    
                    def render_sent_bar(label, count, color_class):
                        pct = (count / total * 100) if total > 0 else 0
                        with ui.row().classes('w-full items-center gap-2 mb-1'):
                            ui.label(label).classes('w-14 font-bold text-xs text-gray-600')
                            with ui.element('div').classes('flex-1 h-2 bg-gray-200 rounded-full overflow-hidden'):
                                ui.element('div').classes(f'h-full {color_class}').style(f'width: {pct}%')
                            ui.label(f"{int(pct)}%").classes('w-8 text-right text-xs font-bold')

                    render_sent_bar("Positive", counts.get('Positive', 0), "bg-green-500")
                    render_sent_bar("Neutral", counts.get('Neutral', 0), "bg-gray-500")
                    render_sent_bar("Negative", counts.get('Negative', 0), "bg-red-500")
                    
                    ui.label(f"Total: {total} tweets").classes('text-xs text-gray-400 mt-1')
                else:
                    ui.label("No data for analytics.").classes('text-gray-500 text-sm')

            # Footer
            ui.label("© 2025 CISF Monitor").classes('text-xs text-gray-400 px-2 mt-2')

    @ui.refreshable
    def content_area():
        view = state['current_view']
        df = db_fetch_tweets(state['query'])
        
        if not df.empty:
            df['datetime_dt'] = pd.to_datetime(df['datetime'], errors='coerce', utc=True)
            df.dropna(subset=['datetime_dt'], inplace=True)
            df.sort_values(by='datetime_dt', ascending=False, inplace=True)
            df['display_date'] = df['datetime_dt'].dt.tz_convert('Asia/Kolkata')
            alert = detect_anomalies(df)
            if alert: ui.notify(alert, color='warning', timeout=5000)

        # Title for the View
        with ui.row().classes('w-full items-center justify-between mb-4 sticky top-0 bg-white/80 backdrop-blur z-10 p-2 border-b border-gray-200'):
            ui.label(view).classes('text-xl font-bold text-gray-900')
            if view == 'Feed':
                with ui.row().classes('items-center gap-2'):
                    ui.switch(value=ar_state['active'], on_change=lambda e: ar_state.update({'active': e.value})).props('dense color=green')
                    state['timer_label'] = ui.label(f"{ar_state['seconds']}s").classes('text-xs font-mono w-6')
                    ui.button(icon='refresh', on_click=handle_quick_update).props('flat round dense color=primary')
        
        if view == 'Feed':
            if state['autopilot']:
                ui.html('<div class="pulse-container"><div class="pulse-particle"></div></div>')
            
            # Skeleton Loader State (only if explicit loading)
            if state['is_loading'] and df.empty:
                with ui.column().classes('w-full gap-4'):
                    for _ in range(3):
                        ui.element('div').classes('skeleton w-full h-32')
            
            elif df.empty:
                with ui.column().classes('w-full items-center justify-center py-20 opacity-60'):
                    ui.icon('search_off', size='4rem').classes('text-gray-300')
                    ui.label("No Tweets Found").classes('text-lg font-bold text-gray-500')
            else:
                now_24h = datetime.now(timezone.utc) - timedelta(hours=24)
                feed_df = df[df['datetime_dt'] >= now_24h]
                for _, row in feed_df.iterrows():
                    render_tweet_card(row, is_new=(row['id'] in state['new_tweet_ids']))
        
        elif view == 'Analytics':
            period_select = ui.select(['24 Hours', '7 Days'], value='24 Hours', label='Timeframe').classes('mb-4 w-48')
            if not df.empty:
                hours = 24 if period_select.value == '24 Hours' else 168
                cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
                ana_df = df[df['datetime_dt'] >= cutoff].copy()
                
                # KPI ROW
                with ui.row().classes('w-full gap-4 mb-6'):
                    with ui.card().classes('premium-metric-card flex-1'):
                        ui.label('Total Volume').classes('text-sm text-gray-500')
                        ui.label(str(len(ana_df))).classes('text-3xl font-bold')
                    
                    # 3. Added Positive Sentiment Card
                    with ui.card().classes('premium-metric-card flex-1'):
                        pos = len(ana_df[ana_df['sentiment']=='Positive'])
                        ui.label('Positive').classes('text-sm text-gray-500')
                        ui.label(str(pos)).classes('text-3xl font-bold text-green-600')

                    with ui.card().classes('premium-metric-card flex-1'):
                        neg = len(ana_df[ana_df['sentiment']=='Negative'])
                        ui.label('Negative').classes('text-sm text-gray-500')
                        ui.label(str(neg)).classes('text-3xl font-bold text-red-600')
                
                # Charts
                with ui.column().classes('chart-container w-full h-80 mb-6'):
                    ui.label('Volume Over Time').classes('font-bold mb-2')
                    freq = 'h' if hours <= 48 else 'D'
                    timeline = ana_df.set_index('datetime_dt').groupby([pd.Grouper(freq=freq), 'sentiment']).size().unstack(fill_value=0).reset_index()
                    cats = timeline['datetime_dt'].dt.strftime('%H:%M' if freq=='h' else '%Y-%m-%d').tolist()
                    series = []
                    colors = {'Positive': '#00ba7c', 'Negative': '#f91880', 'Neutral': '#71767b'}
                    for s in ['Negative', 'Neutral', 'Positive']:
                        if s in timeline.columns:
                            series.append({'name': s, 'type': 'bar', 'stack': 'total', 'color': colors[s], 'data': timeline[s].tolist()})
                    ui.echart({'xAxis': {'type': 'category', 'data': cats}, 'yAxis': {'type': 'value'}, 'series': series, 'tooltip': {'trigger': 'axis'}}).classes('w-full h-full')

                # Risks & Issues Grid
                with ui.grid(columns=2).classes('w-full gap-6 mb-6'):
                    # Sentiment Distribution (Pie Chart)
                    with ui.column().classes('chart-container'):
                        ui.label('Sentiment Overview').classes('font-bold text-blue-600 mb-2')
                        sent_counts = ana_df['sentiment'].value_counts().reset_index()
                        sent_counts.columns = ['Sentiment', 'Count']
                        colors_pie = {'Positive': '#00ba7c', 'Negative': '#f91880', 'Neutral': '#71767b'}
                        
                        ui.echart({
                            'tooltip': {'trigger': 'item'},
                            'legend': {'bottom': '0'},
                            'series': [{
                                'type': 'pie',
                                'radius': ['40%', '70%'],
                                'avoidLabelOverlap': False,
                                'itemStyle': {'borderRadius': 10, 'borderColor': '#fff', 'borderWidth': 2},
                                'data': [{'value': r['Count'], 'name': r['Sentiment'], 'itemStyle': {'color': colors_pie.get(r['Sentiment'], '#gray')}} for _, r in sent_counts.iterrows()]
                            }]
                        }).classes('w-full h-64')
                    
                    # Top Issues
                    with ui.column().classes('chart-container'):
                        ui.label('Top Complaints').classes('font-bold text-orange-500 mb-2')
                        ana_df['issue'] = ana_df['content'].apply(extract_issue)
                        issue_counts = ana_df['issue'].value_counts().head(5).reset_index()
                        issue_counts.columns = ['Issue', 'Count']
                        
                        ui.echart({
                            'tooltip': {'trigger': 'item'},
                            'series': [{
                                'type': 'pie',
                                'radius': ['40%', '70%'],
                                'data': [{'value': r['Count'], 'name': r['Issue']} for _, r in issue_counts.iterrows()]
                            }]
                        }).classes('w-full h-48')

                # --- NEW: SENTIMENT HEATMAP ---
                with ui.column().classes('chart-container w-full h-80 mb-6'):
                    ui.label('Sentiment Activity Heatmap (Day vs Hour)').classes('font-bold mb-2')
                    ana_df['day_name'] = ana_df['datetime_dt'].dt.day_name()
                    ana_df['hour'] = ana_df['datetime_dt'].dt.hour
                    heatmap_data = ana_df.groupby(['day_name', 'hour']).size().unstack(fill_value=0)
                    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                    heatmap_data = heatmap_data.reindex(days_order).fillna(0)
                    
                    # Ensure all hours exist
                    for h in range(24):
                        if h not in heatmap_data.columns: heatmap_data[h] = 0
                    heatmap_data = heatmap_data.sort_index(axis=1)
                    
                    heatmap_vals = []
                    for day_idx, day in enumerate(days_order):
                        if day in heatmap_data.index:
                            for hour in range(24):
                                val = int(heatmap_data.loc[day, hour]) if hour in heatmap_data.columns else 0
                                heatmap_vals.append([hour, day_idx, val])
                                
                    ui.echart({
                        'tooltip': {'position': 'top'},
                        'grid': {'height': '50%', 'top': '10%'},
                        'xAxis': {'type': 'category', 'data': [str(i) for i in range(24)], 'splitArea': {'show': True}},
                        'yAxis': {'type': 'category', 'data': days_order, 'splitArea': {'show': True}},
                        'visualMap': {'min': 0, 'max': heatmap_data.max().max() if not heatmap_data.empty else 10, 'calculable': True, 'orient': 'horizontal', 'left': 'center', 'bottom': '15%'},
                        'series': [{'name': 'Tweets', 'type': 'heatmap', 'data': heatmap_vals, 'label': {'show': False}}]
                    }).classes('w-full h-full')

                # --- NEW: NETWORK GRAPH (BOT DETECTION) ---
                with ui.column().classes('chart-container w-full h-96 mb-6'):
                    ui.label('🕸️ Network Analysis (Bot/Coordination)').classes('font-bold mb-2')
                    
                    edges = []
                    nodes = set()
                    
                    for _, row in ana_df.head(100).iterrows(): # Limit for performance
                        author = row['author_username']
                        nodes.add(author)
                        # Extract mentions
                        mentions = re.findall(r'@(\w+)', row['content'])
                        for m in mentions:
                            nodes.add(m)
                            edges.append({"source": author, "target": m})
                    
                    nodes_list = [{"name": n, "symbolSize": 10} for n in list(nodes)[:50]] 
                    edge_counts = Counter([(e['source'], e['target']) for e in edges])
                    links_list = [{"source": s, "target": t, "value": c} for (s, t), c in edge_counts.most_common(100)]
                    
                    if nodes_list:
                        ui.echart({
                            'tooltip': {},
                            'series': [{
                                'type': 'graph',
                                'layout': 'force',
                                'data': nodes_list,
                                'links': links_list,
                                'roam': True,
                                'label': {'show': True, 'position': 'right'},
                                'force': {'repulsion': 100}
                            }]
                        }).classes('w-full h-full')
                    else:
                         ui.label("No network connections found.").classes('text-gray-500')

                # Influencer Report
                with ui.column().classes('chart-container w-full mb-6'):
                    ui.label('📢 Influencer & Amplifier Report').classes('font-bold mb-2')
                    influencer_df = ana_df.groupby('author_username').agg(
                        reach=('followers', 'max'),
                        tweets=('id', 'count'),
                        sentiment_mode=('sentiment', lambda x: x.mode()[0] if not x.mode().empty else 'Neutral')
                    ).reset_index().sort_values('reach', ascending=False).head(5)
                    
                    cols_inf = [
                        {'name': 'u', 'label': 'User', 'field': 'author_username', 'align': 'left'},
                        {'name': 'r', 'label': 'Followers', 'field': 'reach'},
                        {'name': 't', 'label': 'Tweets', 'field': 'tweets'},
                        {'name': 's', 'label': 'Sentiment', 'field': 'sentiment_mode'}
                    ]
                    ui.table(columns=cols_inf, rows=influencer_df.to_dict('records')).classes('w-full shadow-none')

                # Alert Log
                with ui.column().classes('alert-log-container w-full h-64 mb-6'):
                    with ui.row().classes('items-center gap-2 mb-2'):
                        ui.icon('notifications_active', size='sm').classes('text-red-500 animate-pulse')
                        ui.label('Live Intelligence Alerts').classes('font-bold')
                    
                    with ui.scroll_area().classes('w-full h-full pr-4'):
                        alerts = []
                        neg_count = len(ana_df[ana_df['sentiment']=='Negative'])
                        if neg_count > 5: alerts.append({"msg": f"High negative volume ({neg_count})", "level": "High"})
                        
                        # High reach user check
                        for _, row in influencer_df.iterrows():
                            if row['reach'] > 10000:
                                alerts.append({"msg": f"High reach user @{row['author_username']} active", "level": "Med"})

                        if not alerts: ui.label("No active alerts.").classes('text-gray-500 italic')
                        for a in alerts:
                            color = "text-red-600" if a['level'] == "High" else "text-orange-500"
                            with ui.row().classes('alert-item w-full justify-between'):
                                ui.label(a['msg']).classes('text-sm')
                                ui.label(a['level']).classes(f'font-bold text-xs {color}')

                # System Health
                with ui.expansion('🔌 System Health: API Key Status', icon='dns').classes('w-full border rounded-xl bg-white'):
                    with db_connect() as conn:
                        key_stats = pd.read_sql_query("SELECT id, usage_count, failure_count, last_used FROM api_keys", conn)
                        if not key_stats.empty:
                            ui.table(
                                columns=[
                                    {'name': 'id', 'label': 'ID', 'field': 'id', 'align': 'left'},
                                    {'name': 'used', 'label': 'Used', 'field': 'usage_count'},
                                    {'name': 'fail', 'label': 'Fails', 'field': 'failure_count', 'classes': 'text-red-500 font-bold'},
                                    {'name': 'last', 'label': 'Last Used', 'field': 'last_used'}
                                ],
                                rows=key_stats.to_dict('records')
                            ).classes('w-full shadow-none')
                        else: ui.label("No API stats recorded.").classes('p-4 text-gray-500')
        
    # --- MAIN LAYOUT LOGIC ---
    @ui.refreshable
    def main_layout():
        is_feed = state['current_view'] == 'Feed'
        
        # Main Row container: justify-start keeps content next to left sidebar
        with ui.row().classes('w-full min-h-screen justify-start items-start gap-0 flex-nowrap'):
            
            # --- MAIN CONTENT COLUMN ---
            # Dynamic Width Logic:
            # If Feed: Limited width (half/600px), closer to left.
            # If Other: Expanded width (75%/max-w-5xl).
            content_classes = 'min-h-screen shrink-0 transition-all duration-300 '
            if is_feed:
                content_classes += 'w-full max-w-[600px] border-x border-gray-200'
            else:
                content_classes += 'w-full max-w-5xl p-4 border-l border-gray-200' # Wider for Analytics

            with ui.column().classes(content_classes):
                # Mobile Header (Visible on small screens)
                with ui.row().classes('lg:hidden w-full items-center justify-between p-4 border-b border-gray-200 bg-white/80 backdrop-blur sticky top-0 z-20'):
                    ui.button(icon='menu', on_click=left_drawer.toggle).props('flat round dense color=black')
                    # Mobile Header Logo + Title
                    with ui.row().classes('items-center'):
                        ui.image('logo.png').classes('w-8 h-8 mr-2 object-contain')
                        ui.label('CISF Intel').classes('font-bold text-lg')
                    ui.button(icon='refresh', on_click=handle_quick_update).props('flat round dense color=primary')
                
                loading_overlay()
                content_area()

            # --- RIGHT SIDEBAR (Only visible in Feed View) ---
            if is_feed:
                right_sidebar_content()

    # --- LAYOUT CONSTRUCTION (X-Style) ---
    
    # 1. Left Drawer (Responsive Sidebar)
    with ui.left_drawer(value=True).classes('custom-sidebar bg-white').props('width=280 breakpoint=1024 bordered') as left_drawer:
        with ui.column().classes('w-full h-full p-4 justify-between'):
            with ui.column().classes('w-full gap-2'):
                with ui.column().classes('items-center mb-6 pl-2'):
                    # Logo first
                    ui.image('logo.png').classes('w-28 h-auto logo-normal mt-4')
                    # Text second
                    ui.label('CISF Monitoring Tool').classes('text-lg font-bold text-blue-900 text-center leading-tight mt-4')

                def nav_btn(label, icon, view_target):
                    active = state['current_view'] == view_target
                    cls = 'nav-btn w-full flex items-center'
                    if active: cls += ' active bg-gray-100'
                    ui.button(label, icon=icon, on_click=lambda: set_view(view_target)).classes(cls).props('flat')

                nav_btn('Feed', 'home', 'Feed')
                nav_btn('Analytics', 'analytics', 'Analytics')
                
                ui.separator().classes('my-4')
                ui.button('Refresh', icon='refresh', on_click=handle_refresh).classes('w-full bg-blue-600 text-white rounded-full py-3 font-bold shadow-md hover:bg-blue-700')

            with ui.column().classes('w-full gap-4'):
                with ui.expansion('Settings', icon='settings').classes('w-full border rounded-xl'):
                    with ui.column().classes('p-2'):
                        ui.input(label="Search Query", value=state['query'], on_change=lambda e: (state.update({'query': e.value}), right_sidebar_content.refresh(), main_layout.refresh())).classes('w-full')
                        ui.switch("Official API", value=state['use_x_api'], on_change=lambda e: state.update({'use_x_api': e.value})).props('dense')
                        ui.switch('Auto Pilot', value=state['autopilot'], on_change=lambda e: toggle_autopilot(e.value)).props('dense')
                        ui.switch('Dark Mode').bind_value(dark, 'value').props('dense') # Dark Mode Switch
                
                block_input = ui.input(placeholder="Block ID").classes('w-full text-sm')
                ui.button("Block", on_click=handle_block_submit).classes('w-full text-red-500 border-red-200').props('outline rounded-full dense')

    # 2. Render Main Layout
    main_layout()

port = int(os.environ.get("PORT", 8080))

ui.run(
    host="0.0.0.0",
    port=port,
    title="CISF Monitor Pro",
    favicon="🛡️",
    reload=False

)
