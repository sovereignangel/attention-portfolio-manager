# src/data/journal_parser.py
import json
import os
import pandas as pd
import datetime
import re
from pathlib import Path
import logging
from tqdm import tqdm
import sqlite3
import textblob

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DayOneParser:
    def __init__(self, journal_path=None, db_path='ppm.db'):
        """Initialize DayOne journal parser.
        
        Args:
            journal_path: Path to DayOne export JSON file
            db_path: Path to SQLite database
        """
        self.journal_path = journal_path
        self.db_path = db_path
        self.entries = []
        
    def load_journal(self, journal_path=None):
        """Load and parse DayOne journal export.
        
        Day One exports come in a JSON format with entries and metadata.
        """
        if journal_path:
            self.journal_path = journal_path
            
        if not self.journal_path:
            logger.error("No journal path specified")
            return False
            
        try:
            # Check if it's a directory or file
            path = Path(self.journal_path)
            
            if path.is_dir():
                # Look for JSON files in directory
                json_files = list(path.glob("*.json"))
                if not json_files:
                    logger.error(f"No JSON files found in {self.journal_path}")
                    return False
                    
                # Use the first JSON file found
                journal_file = json_files[0]
            else:
                journal_file = path
                
            logger.info(f"Loading journal from {journal_file}")
            
            with open(journal_file, 'r', encoding='utf-8') as f:
                journal_data = json.load(f)
                
            # Extract entries
            if 'entries' in journal_data:
                self.entries = journal_data['entries']
                logger.info(f"Loaded {len(self.entries)} journal entries")
                return True
            else:
                logger.error("No entries found in journal file")
                return False
                
        except Exception as e:
            logger.error(f"Error loading journal: {e}")
            return False
            
    def process_entries(self):
        """Process journal entries into a DataFrame with analysis."""
        if not self.entries:
            logger.warning("No entries to process")
            return pd.DataFrame()
            
        processed_entries = []
        
        for entry in tqdm(self.entries, desc="Processing journal entries"):
            try:
                # Basic entry metadata
                entry_id = entry.get('uuid', '')
                created_date = entry.get('creationDate', '')
                modified_date = entry.get('modifiedDate', '')
                text = entry.get('text', '')
                
                # Convert to datetime object
                if created_date:
                    created_dt = datetime.datetime.fromisoformat(
                        created_date.replace('Z', '+00:00')
                    )
                else:
                    created_dt = None
                    
                # Get tags
                tags = entry.get('tags', [])
                tags_str = ', '.join(tags) if tags else ''
                
                # Extract location data if available
                location = entry.get('location', {})
                place_name = location.get('placeName', '')
                
                # Perform sentiment analysis
                if text:
                    blob = textblob.TextBlob(text)
                    sentiment_polarity = blob.sentiment.polarity  # -1 to 1
                    sentiment_subjectivity = blob.sentiment.subjectivity  # 0 to 1
                    
                    # Classify general sentiment
                    if sentiment_polarity > 0.2:
                        sentiment = 'positive'
                    elif sentiment_polarity < -0.2:
                        sentiment = 'negative'
                    else:
                        sentiment = 'neutral'
                        
                    # Extract word count and basic text metrics
                    word_count = len(text.split())
                    
                    # Look for emotion-related keywords
                    emotion_keywords = {
                        'joy': ['happy', 'joy', 'excited', 'glad', 'thankful', 'grateful', 'delighted'],
                        'sadness': ['sad', 'down', 'depressed', 'unhappy', 'miserable', 'disappointed'],
                        'anger': ['angry', 'frustrated', 'annoyed', 'irritated', 'upset', 'mad'],
                        'fear': ['afraid', 'scared', 'anxious', 'nervous', 'worried', 'fearful'],
                        'surprise': ['surprised', 'shocked', 'amazed', 'astonished'],
                        'love': ['love', 'adore', 'cherish', 'caring', 'affection'],
                        'gratitude': ['grateful', 'thankful', 'appreciative', 'blessed']
                    }
                    
                    detected_emotions = []
                    text_lower = text.lower()
                    
                    for emotion, keywords in emotion_keywords.items():
                        if any(keyword in text_lower for keyword in keywords):
                            detected_emotions.append(emotion)
                    
                    primary_emotion = detected_emotions[0] if detected_emotions else 'neutral'
                    all_emotions = ', '.join(detected_emotions) if detected_emotions else 'neutral'
                    
                    # Look for activity mentions
                    activity_patterns = {
                        'Generation': [r'\b(writ|creat|build|develop|cod|design|work)\w*\b'],
                        'Charging': [r'\b(rest|sleep|meditat|relax|recov|break)\w*\b'],
                        'Growth': [r'\b(learn|stud|read|cours|train|develop)\w*\b'],
                        'Connection': [r'\b(meet|call|chat|coffee|lunch|dinner|social|friend|family)\w*\b'],
                        'Vitality': [r'\b(gym|work\sout|exercis|run|jog|yoga|health)\w*\b']
                    }
                    
                    detected_activities = []
                    for activity, patterns in activity_patterns.items():
                        for pattern in patterns:
                            if re.search(pattern, text_lower):
                                detected_activities.append(activity)
                                break
                                
                    activities = ', '.join(set(detected_activities)) if detected_activities else 'Other'
                else:
                    sentiment_polarity = 0
                    sentiment_subjectivity = 0
                    sentiment = 'unknown'
                    word_count = 0
                    primary_emotion = 'unknown'
                    all_emotions = 'unknown'
                    activities = 'Other'
                
                # Create entry record
                entry_record = {
                    'entry_id': entry_id,
                    'created_date': created_dt,
                    'tags': tags_str,
                    'location': place_name,
                    'word_count': word_count,
                    'sentiment_polarity': sentiment_polarity,
                    'sentiment_subjectivity': sentiment_subjectivity,
                    'sentiment': sentiment,
                    'primary_emotion': primary_emotion,
                    'all_emotions': all_emotions,
                    'activities': activities,
                    'text': text[:500]  # Truncate for dataframe display
                }
                
                processed_entries.append(entry_record)
                
            except Exception as e:
                logger.warning(f"Error processing entry {entry.get('uuid', 'unknown')}: {e}")
                continue
                
        # Convert to DataFrame
        df = pd.DataFrame(processed_entries)
        return df
        
    def save_to_database(self, entries_df):
        """Save processed journal entries to database."""
        if entries_df.empty:
            logger.warning("No entries to save to database")
            return False
            
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create journal entries table if it doesn't exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS journal_entries (
                id INTEGER PRIMARY KEY,
                entry_id TEXT UNIQUE,
                created_date TIMESTAMP,
                tags TEXT,
                location TEXT,
                word_count INTEGER,
                sentiment_polarity REAL,
                sentiment_subjectivity REAL,
                sentiment TEXT,
                primary_emotion TEXT,
                all_emotions TEXT,
                activities TEXT,
                text TEXT
            )
            ''')
            
            # Create emotion metrics table if it doesn't exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS emotion_metrics (
                id INTEGER PRIMARY KEY,
                date DATE UNIQUE,
                joy REAL,
                sadness REAL,
                anger REAL,
                fear REAL,
                surprise REAL,
                love REAL,
                gratitude REAL,
                overall_sentiment REAL
            )
            ''')
            
            # Save entries
            for _, entry in tqdm(entries_df.iterrows(), total=len(entries_df), desc="Saving journal entries"):
                # Check if entry already exists
                cursor.execute(
                    "SELECT id FROM journal_entries WHERE entry_id = ?",
                    (entry['entry_id'],)
                )
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing entry
                    cursor.execute(
                        """
                        UPDATE journal_entries
                        SET created_date = ?, tags = ?, location = ?, word_count = ?,
                            sentiment_polarity = ?, sentiment_subjectivity = ?,
                            sentiment = ?, primary_emotion = ?, all_emotions = ?,
                            activities = ?, text = ?
                        WHERE entry_id = ?
                        """,
                        (
                            entry['created_date'],
                            entry['tags'],
                            entry['location'],
                            entry['word_count'],
                            entry['sentiment_polarity'],
                            entry['sentiment_subjectivity'],
                            entry['sentiment'],
                            entry['primary_emotion'],
                            entry['all_emotions'],
                            entry['activities'],
                            entry['text'],
                            entry['entry_id']
                        )
                    )
                else:
                    # Insert new entry
                    cursor.execute(
                        """
                        INSERT INTO journal_entries
                        (entry_id, created_date, tags, location, word_count,
                         sentiment_polarity, sentiment_subjectivity, sentiment,
                         primary_emotion, all_emotions, activities, text)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            entry['entry_id'],
                            entry['created_date'],
                            entry['tags'],
                            entry['location'],
                            entry['word_count'],
                            entry['sentiment_polarity'],
                            entry['sentiment_subjectivity'],
                            entry['sentiment'],
                            entry['primary_emotion'],
                            entry['all_emotions'],
                            entry['activities'],
                            entry['text']
                        )
                    )
            
            # Generate daily emotion metrics
            cursor.execute('''
            SELECT 
                DATE(created_date) as entry_date,
                AVG(CASE WHEN primary_emotion = 'joy' THEN 1 ELSE 0 END) as joy,
                AVG(CASE WHEN primary_emotion = 'sadness' THEN 1 ELSE 0 END) as sadness,
                AVG(CASE WHEN primary_emotion = 'anger' THEN 1 ELSE 0 END) as anger,
                AVG(CASE WHEN primary_emotion = 'fear' THEN 1 ELSE 0 END) as fear,
                AVG(CASE WHEN primary_emotion = 'surprise' THEN 1 ELSE 0 END) as surprise,
                AVG(CASE WHEN primary_emotion = 'love' THEN 1 ELSE 0 END) as love,
                AVG(CASE WHEN primary_emotion = 'gratitude' THEN 1 ELSE 0 END) as gratitude,
                AVG(sentiment_polarity) as overall_sentiment
            FROM journal_entries
            GROUP BY DATE(created_date)
            ''')
            
            daily_metrics = cursor.fetchall()
            
            # Save daily emotion metrics
            for metrics in daily_metrics:
                entry_date, joy, sadness, anger, fear, surprise, love, gratitude, overall_sentiment = metrics
                
                # Check if metrics for this date already exist
                cursor.execute(
                    "SELECT id FROM emotion_metrics WHERE date = ?",
                    (entry_date,)
                )
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing metrics
                    cursor.execute(
                        """
                        UPDATE emotion_metrics
                        SET joy = ?, sadness = ?, anger = ?, fear = ?,
                            surprise = ?, love = ?, gratitude = ?, overall_sentiment = ?
                        WHERE date = ?
                        """,
                        (joy, sadness, anger, fear, surprise, love, gratitude, overall_sentiment, entry_date)
                    )
                else:
                    # Insert new metrics
                    cursor.execute(
                        """
                        INSERT INTO emotion_metrics
                        (date, joy, sadness, anger, fear, surprise, love, gratitude, overall_sentiment)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (entry_date, joy, sadness, anger, fear, surprise, love, gratitude, overall_sentiment)
                    )
            
            # Update outcome_metrics table with emotion data
            cursor.execute('''
            SELECT date, overall_sentiment FROM emotion_metrics
            ''')
            
            sentiment_data = cursor.fetchall()
            
            for date_str, sentiment in sentiment_data:
                # Convert sentiment scale from -1:1 to 1:10
                happiness_score = int((sentiment + 1) * 5)
                
                # Check if date exists in outcome_metrics
                cursor.execute(
                    "SELECT metric_id FROM outcome_metrics WHERE date = ?",
                    (date_str,)
                )
                existing = cursor.fetchone()
                
                if existing:
                    # Update happiness score
                    cursor.execute(
                        "UPDATE outcome_metrics SET happiness = ? WHERE date = ?",
                        (happiness_score, date_str)
                    )
                else:
                    # Create new record
                    cursor.execute(
                        "INSERT INTO outcome_metrics (date, happiness) VALUES (?, ?)",
                        (date_str, happiness_score)
                    )
            
            conn.commit()
            conn.close()
            
            logger.info(f"Successfully saved {len(entries_df)} journal entries to database")
            return True
            
        except Exception as e:
            logger.error(f"Error saving journal entries to database: {e}")
            return False

    def run_full_import(self, journal_path=None):
        """Run full journal import process."""
        if journal_path:
            self.journal_path = journal_path
            
        if not self.load_journal():
            return False
            
        entries_df = self.process_entries()
        if entries_df.empty:
            return False
            
        return self.save_to_database(entries_df)
    