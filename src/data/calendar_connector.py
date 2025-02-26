# src/data/calendar_connector.py
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import os.path
import pickle
import datetime
import pandas as pd
from tqdm import tqdm
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GoogleCalendarConnector:
    def __init__(self, credentials_path='credentials.json', token_path='token.pickle'):
        """Initialize Google Calendar connector with authentication paths."""
        self.credentials_path = credentials_path
        self.token_path = token_path
        # If modifying these scopes, delete the token.pickle file
        self.SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
        self.service = self._authenticate()
        
    def _authenticate(self):
        """Authenticate with Google Calendar API."""
        creds = None
        
        # Check if token file exists
        if os.path.exists(self.token_path):
            with open(self.token_path, 'rb') as token:
                creds = pickle.load(token)
                
        # If credentials don't exist or are invalid, get new ones
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, self.SCOPES)
                creds = flow.run_local_server(port=0)
                
            # Save credentials for future use
            with open(self.token_path, 'wb') as token:
                pickle.dump(creds, token)
                
        return build('calendar', 'v3', credentials=creds)
    
    def get_calendar_list(self):
        """Get list of all available calendars."""
        try:
            calendar_list = self.service.calendarList().list().execute()
            calendars = [(cal['id'], cal.get('summary', 'Unknown')) for cal in calendar_list.get('items', [])]
            return calendars
        except Exception as e:
            logger.error(f"Error fetching calendar list: {e}")
            return []
    
    def fetch_events(self, calendar_id='primary', days_back=90, days_forward=7):
        """Fetch events from specified calendar for the given time range."""
        # Calculate time range
        now = datetime.datetime.utcnow()
        start_time = (now - datetime.timedelta(days=days_back)).isoformat() + 'Z'
        end_time = (now + datetime.timedelta(days=days_forward)).isoformat() + 'Z'
        
        logger.info(f"Fetching events from {days_back} days ago to {days_forward} days ahead")
        
        try:
            events_result = self.service.events().list(
                calendarId=calendar_id,
                timeMin=start_time,
                timeMax=end_time,
                maxResults=2500,
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            
            events = events_result.get('items', [])
            logger.info(f"Found {len(events)} events")
            
            if not events:
                return pd.DataFrame()
                
            return self._process_events(events)
            
        except Exception as e:
            logger.error(f"Error fetching events: {e}")
            return pd.DataFrame()
    
    def _process_events(self, events):
        """Process raw event data into a structured DataFrame."""
        event_data = []
        
        for event in tqdm(events, desc="Processing events"):
            try:
                # Extract basic event info
                event_id = event.get('id', '')
                summary = event.get('summary', 'No Title')
                description = event.get('description', '')
                location = event.get('location', '')
                
                # Handle start and end times (accounting for all-day events)
                start_info = event.get('start', {})
                end_info = event.get('end', {})
                
                # Check if it's an all-day event
                is_all_day = 'date' in start_info and 'date' in end_info
                
                # Process start time
                if is_all_day:
                    start_dt = datetime.datetime.fromisoformat(start_info['date'])
                    end_dt = datetime.datetime.fromisoformat(end_info['date'])
                else:
                    start_dt = datetime.datetime.fromisoformat(
                        start_info.get('dateTime', '').replace('Z', '+00:00')
                    )
                    end_dt = datetime.datetime.fromisoformat(
                        end_info.get('dateTime', '').replace('Z', '+00:00')
                    )
                
                # Calculate duration in minutes
                duration_minutes = (end_dt - start_dt).total_seconds() / 60
                
                # Additional metadata
                attendees = event.get('attendees', [])
                num_attendees = len(attendees)
                is_recurring = 'recurringEventId' in event
                status = event.get('status', 'unknown')
                
                # Creator & organizer info
                creator = event.get('creator', {}).get('email', 'unknown')
                organizer = event.get('organizer', {}).get('email', 'unknown')
                
                # Self-created event check
                is_self_created = creator == organizer
                
                # Create event record
                event_record = {
                    'event_id': event_id,
                    'summary': summary,
                    'description': description,
                    'location': location,
                    'start_time': start_dt,
                    'end_time': end_dt,
                    'duration_minutes': duration_minutes,
                    'is_all_day': is_all_day,
                    'num_attendees': num_attendees,
                    'is_recurring': is_recurring,
                    'status': status,
                    'is_self_created': is_self_created
                }
                
                event_data.append(event_record)
                
            except Exception as e:
                logger.warning(f"Error processing event {event.get('id', 'unknown')}: {e}")
                continue
        
        # Convert to DataFrame
        df = pd.DataFrame(event_data)
        return df
    
    def categorize_events(self, events_df):
        """Categorize events based on keywords and patterns."""
        if events_df.empty:
            return events_df
            
        # Keywords for categories (expanded for better matching)
        category_keywords = {
            'Generation': [
                'create', 'write', 'draft', 'author', 'build', 'develop', 'code', 
                'design', 'produce', 'generate', 'make', 'craft', 'compose', 'project',
                'implement', 'program', 'construct', 'plan', 'strategize', 'brainstorm',
                'deep work', 'focus', 'flow'
            ],
            'Charging': [
                'rest', 'sleep', 'nap', 'meditate', 'relax', 'recovery', 'recharge',
                'break', 'pause', 'self-care', 'downtime', 'unwind', 'leisure',
                'vacation', 'holiday', 'day off', 'pto', 'personal', 'me time'
            ],
            'Growth': [
                'learn', 'study', 'read', 'course', 'class', 'training', 'development',
                'workshop', 'seminar', 'conference', 'webinar', 'education', 'skill',
                'improve', 'growth', 'progress', 'lecture', 'tutorial', 'lesson',
                'research', 'explore', 'practice'
            ],
            'Connection': [
                'meet', 'call', 'chat', 'coffee', 'lunch', 'dinner', 'social',
                'friend', 'family', 'network', 'connect', 'relationship', 'date',
                'party', 'gathering', 'celebration', 'hangout', 'catch up', 'reunion',
                'zoom', 'teams', 'google meet', 'facetime', '1:1', '1-1', 'one on one'
            ],
            'Vitality': [
                'gym', 'workout', 'exercise', 'run', 'jog', 'yoga', 'pilates', 'health',
                'doctor', 'medical', 'dentist', 'therapy', 'fitness', 'swim', 'bike',
                'hike', 'walk', 'sport', 'train', 'physical', 'nutrition', 'diet',
                'massage', 'stretch', 'wellness'
            ]
        }
        
        # Function to match event to category
        def match_category(row):
            text = f"{row['summary']} {row['description']}".lower()
            
            # Check against each category's keywords
            for category, keywords in category_keywords.items():
                if any(keyword in text for keyword in keywords):
                    return category
            
            # Default category if no match found
            return 'Other'
        
        # Apply categorization
        events_df['category'] = events_df.apply(match_category, axis=1)
        
        # Calculate additional metrics for analysis
        events_df['day_of_week'] = events_df['start_time'].dt.day_name()
        events_df['hour_of_day'] = events_df['start_time'].dt.hour
        events_df['week_number'] = events_df['start_time'].dt.isocalendar().week
        events_df['month'] = events_df['start_time'].dt.month_name()
        
        return events_df
    
    def save_to_database(self, events_df, db_path='ppm.db'):
        """Save processed events to the database."""
        if events_df.empty:
            logger.warning("No events to save to database")
            return False
            
        try:
            import sqlite3
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # First ensure all categories exist in activities table
            categories = events_df['category'].unique()
            
            for category in categories:
                # Check if category exists
                cursor.execute(
                    "SELECT COUNT(*) FROM activities WHERE name = ? AND domain = ?",
                    (category, category)
                )
                exists = cursor.fetchone()[0] > 0
                
                if not exists:
                    cursor.execute(
                        """
                        INSERT INTO activities (name, description, domain, energy_impact, 
                                              engagement_level, value_alignment)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (category, f"Events categorized as {category}", category, 0, 5, 5)
                    )
            
            # Get activity_ids for all categories
            cursor.execute("SELECT activity_id, name FROM activities")
            activity_map = {name: id for id, name in cursor.fetchall()}
            
            # Process events
            processed_count = 0
            for _, event in tqdm(events_df.iterrows(), total=len(events_df), desc="Saving events"):
                activity_id = activity_map.get(event['category'])
                if not activity_id:
                    logger.warning(f"No activity_id found for category: {event['category']}")
                    continue
                
                # Check if event already exists
                cursor.execute(
                    "SELECT entry_id FROM time_entries WHERE source = ?",
                    (event['event_id'],)
                )
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing entry
                    cursor.execute(
                        """
                        UPDATE time_entries 
                        SET activity_id = ?, start_time = ?, end_time = ?, 
                            duration = ?, notes = ?, source = ?
                        WHERE source = ?
                        """,
                        (
                            activity_id,
                            event['start_time'],
                            event['end_time'],
                            event['duration_minutes'],
                            f"{event['summary']} - {event['description']}",
                            event['event_id'],
                            event['event_id']
                        )
                    )
                else:
                    # Insert new entry
                    cursor.execute(
                        """
                        INSERT INTO time_entries 
                        (activity_id, start_time, end_time, duration, notes, source)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            activity_id,
                            event['start_time'],
                            event['end_time'],
                            event['duration_minutes'],
                            f"{event['summary']} - {event['description']}",
                            event['event_id']
                        )
                    )
                processed_count += 1
            
            conn.commit()
            conn.close()
            
            logger.info(f"Successfully saved {processed_count} events to database")
            return True
            
        except Exception as e:
            logger.error(f"Error saving events to database: {e}")
            return False