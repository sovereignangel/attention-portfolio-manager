import sys
import os
from pathlib import Path

# Get the absolute path to the project root directory
project_root = str(Path(__file__).parent.parent.parent.absolute())

# Add the project root to the Python path
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# src/data/etl_pipeline.py
import logging
import datetime
import os
import sqlite3
import pandas as pd
from pathlib import Path
import time

# Import our connectors
from .calendar_connector import GoogleCalendarConnector
from src.data.journal_parser import DayOneParser  # absolute import

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ppm_etl.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("PPM_ETL")

class DataPipeline:
    def __init__(self, db_path='ppm.db', config=None):
        """Initialize the ETL pipeline.
        
        Args:
            db_path: Path to SQLite database
            config: Configuration dictionary
        """
        self.db_path = db_path
        self.config = config or {}
        self.ensure_database()
        
    def ensure_database(self):
        """Make sure the database exists with all required tables."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create activities table if not exists
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS activities (
                activity_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                domain TEXT NOT NULL,
                energy_impact INTEGER DEFAULT 0,
                engagement_level INTEGER DEFAULT 5,
                time_horizon TEXT,
                input_output_ratio TEXT,
                challenge_level INTEGER,
                social_dimension TEXT,
                intention_level INTEGER,
                value_alignment INTEGER DEFAULT 5,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            # Create time_entries table if not exists
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS time_entries (
                entry_id INTEGER PRIMARY KEY,
                activity_id INTEGER,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP NOT NULL,
                duration INTEGER NOT NULL,
                engagement_rating INTEGER,
                energy_before INTEGER,
                energy_after INTEGER,
                flow_quality INTEGER,
                notes TEXT,
                source TEXT,
                FOREIGN KEY (activity_id) REFERENCES activities (activity_id)
            )
            ''')
            
            # Create outcome_metrics table if not exists
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS outcome_metrics (
                metric_id INTEGER PRIMARY KEY,
                date DATE UNIQUE NOT NULL,
                happiness INTEGER,
                peace INTEGER,
                freedom INTEGER,
                wealth_indicators TEXT,
                generation_output TEXT,
                well_being INTEGER,
                abundance_mindset INTEGER,
                flow_frequency INTEGER
            )
            ''')
            
            # Create journal_entries table if not exists
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
            
            # Create emotion_metrics table if not exists
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
            
            conn.commit()
            conn.close()
            logger.info("Database schema verified and updated if needed")
            
        except Exception as e:
            logger.error(f"Error ensuring database: {e}")
            raise
    
    def run_google_calendar_import(self, days_back=90, days_forward=7, calendar_id='primary', credentials_path='credentials.json'):
        """Import Google Calendar data.
        
        Args:
            days_back: Number of days in the past to import
            days_forward: Number of days in the future to import
            calendar_id: Google Calendar ID to import
            credentials_path: Path to Google API credentials file
            
        Returns:
            bool: Success or failure
        """
        try:
            logger.info(f"Starting Google Calendar import from {days_back} days ago to {days_forward} days ahead")
            
            # Initialize connector
            calendar_connector = GoogleCalendarConnector(credentials_path=credentials_path)
            
            # Fetch and categorize events
            events_df = calendar_connector.fetch_events(
                calendar_id=calendar_id, 
                days_back=days_back,
                days_forward=days_forward
            )
            
            if events_df.empty:
                logger.warning("No calendar events found for the specified period")
                return False
                
            logger.info(f"Found {len(events_df)} calendar events")
            
            # Categorize events
            categorized_df = calendar_connector.categorize_events(events_df)
            
            # Save to database
            success = calendar_connector.save_to_database(categorized_df, db_path=self.db_path)
            
            if success:
                logger.info("Google Calendar import completed successfully")
            else:
                logger.error("Failed to save calendar data to database")
            
            return success
            
        except Exception as e:
            logger.error(f"Error in Google Calendar import: {e}")
            return False
    
    def run_dayone_journal_import(self, journal_path):
        """Import DayOne journal data.
        
        Args:
            journal_path: Path to DayOne export (JSON file or directory)
            
        Returns:
            bool: Success or failure
        """
        try:
            logger.info(f"Starting DayOne journal import from {journal_path}")
            
            # Initialize parser
            journal_parser = DayOneParser(journal_path=journal_path, db_path=self.db_path)
            
            # Run full import process
            success = journal_parser.run_full_import()
            
            if success:
                logger.info("DayOne journal import completed successfully")
            else:
                logger.error("Failed to import DayOne journal")
            
            return success
            
        except Exception as e:
            logger.error(f"Error in DayOne journal import: {e}")
            return False
    
    def calculate_daily_metrics(self):
        """Calculate and update daily metrics for outcome_metrics table.
        
        Takes data from various sources and computes unified daily metrics.
        
        Returns:
            bool: Success or failure
        """
        try:
            logger.info("Calculating daily metrics")
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get date range from existing data
            cursor.execute("""
            SELECT 
                MIN(date(start_time)) as min_date,
                MAX(date(start_time)) as max_date
            FROM time_entries
            """)
            
            date_range = cursor.fetchone()
            if not date_range or not date_range[0]:
                logger.warning("No date range found in time_entries")
                return False
                
            min_date, max_date = date_range
            
            # Create date series
            current_date = datetime.datetime.strptime(min_date, '%Y-%m-%d').date()
            end_date = datetime.datetime.strptime(max_date, '%Y-%m-%d').date()
            
            all_dates = []
            while current_date <= end_date:
                all_dates.append(current_date.strftime('%Y-%m-%d'))
                current_date += datetime.timedelta(days=1)
            
            for date_str in all_dates:
                # Get time allocation data
                cursor.execute("""
                SELECT 
                    a.domain,
                    SUM(te.duration) as minutes
                FROM time_entries te
                JOIN activities a ON te.activity_id = a.activity_id
                WHERE date(te.start_time) = ?
                GROUP BY a.domain
                """, (date_str,))
                
                time_data = cursor.fetchall()
                
                # Process time data
                domains = {}
                total_minutes = 0
                
                for domain, minutes in time_data:
                    domains[domain] = minutes
                    total_minutes += minutes
                
                # Format domain data as JSON
                import json
                domain_json = json.dumps(domains)
                
                # Get emotion data
                cursor.execute("""
                SELECT 
                    overall_sentiment
                FROM emotion_metrics
                WHERE date = ?
                """, (date_str,))
                
                emotion_row = cursor.fetchone()
                sentiment = emotion_row[0] if emotion_row else None
                
                # Convert sentiment to happiness score (1-10)
                happiness = int((sentiment + 1) * 5) if sentiment is not None else None
                
                # Check if we have journal entries for this day
                cursor.execute("""
                SELECT COUNT(*) FROM journal_entries 
                WHERE date(created_date) = ?
                """, (date_str,))
                
                has_journal = cursor.fetchone()[0] > 0
                
                # Calculate generation metrics based on time spent in generation activities
                generation_time = domains.get('Generation', 0)
                generation_score = min(10, int(generation_time / 60)) if generation_time else None
                
                # Calculate peace metric (placeholder - would be better with HRV data)
                peace_score = happiness if happiness else None
                
                # Calculate freedom metric (placeholder)
                freedom_score = 5  # Default middle value
                
                # Update outcome metrics
                cursor.execute("""
                SELECT metric_id FROM outcome_metrics WHERE date = ?
                """, (date_str,))
                
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing record
                    cursor.execute("""
                    UPDATE outcome_metrics
                    SET 
                        happiness = COALESCE(?, happiness),
                        peace = COALESCE(?, peace),
                        freedom = COALESCE(?, freedom),
                        wealth_indicators = ?,
                        generation_output = ?,
                        well_being = COALESCE(?, well_being),
                        abundance_mindset = COALESCE(?, abundance_mindset)
                    WHERE date = ?
                    """, (
                        happiness,
                        peace_score,
                        freedom_score,
                        domain_json,
                        str(generation_score) if generation_score else None,
                        happiness,  # Using happiness as a proxy for well-being
                        happiness,  # Using happiness as a proxy for abundance mindset
                        date_str
                    ))
                else:
                    # Insert new record
                    cursor.execute("""
                    INSERT INTO outcome_metrics
                    (date, happiness, peace, freedom, wealth_indicators, generation_output, well_being, abundance_mindset)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        date_str,
                        happiness,
                        peace_score,
                        freedom_score,
                        domain_json,
                        str(generation_score) if generation_score else None,
                        happiness,  # Using happiness as a proxy for well-being
                        happiness  # Using happiness as a proxy for abundance mindset
                    ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Daily metrics calculated for {len(all_dates)} days")
            return True
            
        except Exception as e:
            logger.error(f"Error calculating daily metrics: {e}")
            return False
    
    def analyze_patterns(self):
        """Analyze patterns in the data and save insights.
        
        This performs basic pattern recognition and correlation analysis.
        
        Returns:
            dict: Dictionary of insights
        """
        try:
            logger.info("Analyzing data patterns")
            
            conn = sqlite3.connect(self.db_path)
            
            # Join time entries with outcome metrics
            query = """
            SELECT 
                date(te.start_time) as day,
                a.domain,
                SUM(te.duration) as minutes,
                om.happiness,
                om.peace,
                om.freedom,
                om.well_being
            FROM time_entries te
            JOIN activities a ON te.activity_id = a.activity_id
            LEFT JOIN outcome_metrics om ON date(te.start_time) = om.date
            WHERE om.happiness IS NOT NULL
            GROUP BY date(te.start_time), a.domain
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if df.empty:
                logger.warning("Not enough data for pattern analysis")
                return {}
                
            # Pivot to get domains as columns
            pivot_df = df.pivot_table(
                index=['day', 'happiness', 'peace', 'freedom', 'well_being'],
                columns='domain',
                values='minutes',
                fill_value=0
            ).reset_index()
            
            # Calculate correlations
            domain_columns = [col for col in pivot_df.columns if col not in ['day', 'happiness', 'peace', 'freedom', 'well_being']]
            
            if not domain_columns or len(pivot_df) < 5:
                logger.warning("Not enough data for correlation analysis")
                return {}
                
            # Calculate correlations with outcome metrics
            correlations = {}
            
            for domain in domain_columns:
                domain_correlations = {}
                for metric in ['happiness', 'peace', 'freedom', 'well_being']:
                    if metric in pivot_df.columns and pivot_df[metric].notna().sum() > 3:
                        corr = pivot_df[domain].corr(pivot_df[metric])
                        if not pd.isna(corr):
                            domain_correlations[metric] = round(corr, 2)
                
                if domain_correlations:
                    correlations[domain] = domain_correlations
            
            # Find activities with strongest positive correlations
            positive_correlations = []
            for domain, metrics in correlations.items():
                for metric, value in metrics.items():
                    if value > 0.3:  # Threshold for meaningful positive correlation
                        positive_correlations.append({
                            'domain': domain,
                            'metric': metric,
                            'correlation': value
                        })
            
            # Find activities with strongest negative correlations
            negative_correlations = []
            for domain, metrics in correlations.items():
                for metric, value in metrics.items():
                    if value < -0.3:  # Threshold for meaningful negative correlation
                        negative_correlations.append({
                            'domain': domain,
                            'metric': metric,
                            'correlation': value
                        })
            
            # Sort by correlation strength
            positive_correlations = sorted(positive_correlations, key=lambda x: x['correlation'], reverse=True)
            negative_correlations = sorted(negative_correlations, key=lambda x: x['correlation'])
            
            insights = {
                'positive_correlations': positive_correlations,
                'negative_correlations': negative_correlations,
                'correlation_matrix': correlations
            }
            
            logger.info(f"Pattern analysis completed with {len(positive_correlations)} positive and {len(negative_correlations)} negative correlations found")
            return insights
            
        except Exception as e:
            logger.error(f"Error analyzing patterns: {e}")
            return {}
    
    def generate_recommendations(self):
        """Generate time allocation recommendations based on insights.
        
        Returns:
            list: List of recommendation dictionaries
        """
        try:
            logger.info("Generating recommendations")
            
            # Get patterns
            patterns = self.analyze_patterns()
            
            if not patterns or not patterns.get('positive_correlations'):
                logger.warning("Not enough pattern data for recommendations")
                return []
                
            # Connect to database
            conn = sqlite3.connect(self.db_path)
            
            # Get current time allocation
            query = """
            SELECT 
                a.domain,
                SUM(te.duration) as minutes
            FROM time_entries te
            JOIN activities a ON te.activity_id = a.activity_id
            WHERE te.start_time >= date('now', '-30 day')
            GROUP BY a.domain
            """
            
            current_allocation = pd.read_sql_query(query, conn)
            conn.close()
            
            if current_allocation.empty:
                logger.warning("No recent time data for recommendations")
                return []
                
            # Calculate total tracked minutes
            total_minutes = current_allocation['minutes'].sum()
            
            if total_minutes == 0:
                logger.warning("Zero total minutes tracked")
                return []
                
            # Calculate percentages
            current_allocation['percentage'] = (current_allocation['minutes'] / total_minutes) * 100
            
            # Convert to dictionary for easier access
            current_alloc_dict = dict(zip(current_allocation['domain'], current_allocation['percentage']))
            
            # Generate recommendations based on positive correlations
            recommendations = []
            
            # Activities strongly correlated with positive outcomes
            high_value_domains = set()
            for corr in patterns['positive_correlations']:
                high_value_domains.add(corr['domain'])
                
            for domain in high_value_domains:
                current_pct = current_alloc_dict.get(domain, 0)
                
                # Metrics this domain correlates with
                related_metrics = [c['metric'] for c in patterns['positive_correlations'] if c['domain'] == domain]
                metrics_text = ', '.join(related_metrics)
                
                # Different recommendation based on current allocation
                if current_pct < 10:
                    recommendations.append({
                        'domain': domain,
                        'action': 'increase',
                        'reason': f"Strongly correlates with {metrics_text}",
                        'current_percentage': round(current_pct, 1),
                        'suggested_percentage': round(min(current_pct * 1.5, current_pct + 15), 1),
                        'priority': 'high' if current_pct < 5 else 'medium'
                    })
                elif current_pct < 25:
                    recommendations.append({
                        'domain': domain,
                        'action': 'maintain',
                        'reason': f"Positively impacts {metrics_text}",
                        'current_percentage': round(current_pct, 1),
                        'suggested_percentage': round(current_pct, 1),
                        'priority': 'medium'
                    })
                else:
                    recommendations.append({
                        'domain': domain,
                        'action': 'optimize',
                        'reason': f"Important for {metrics_text}, but ensure quality over quantity",
                        'current_percentage': round(current_pct, 1),
                        'suggested_percentage': round(current_pct, 1),
                        'priority': 'low'
                    })
            
            # Activities negatively correlated with outcomes
            low_value_domains = set()
            for corr in patterns['negative_correlations']:
                low_value_domains.add(corr['domain'])
                
            for domain in low_value_domains:
                if domain in high_value_domains:
                    # Skip if also in high value (conflicting signals)
                    continue
                    
                current_pct = current_alloc_dict.get(domain, 0)
                
                # Metrics this domain negatively correlates with
                related_metrics = [c['metric'] for c in patterns['negative_correlations'] if c['domain'] == domain]
                metrics_text = ', '.join(related_metrics)
                
                if current_pct > 20:
                    recommendations.append({
                        'domain': domain,
                        'action': 'decrease',
                        'reason': f"Negatively correlates with {metrics_text}",
                        'current_percentage': round(current_pct, 1),
                        'suggested_percentage': round(max(current_pct * 0.7, current_pct - 15), 1),
                        'priority': 'high' if current_pct > 30 else 'medium'
                    })
                elif current_pct > 5:
                    recommendations.append({
                        'domain': domain,
                        'action': 'review',
                        'reason': f"May negatively impact {metrics_text}",
                        'current_percentage': round(current_pct, 1),
                        'suggested_percentage': round(current_pct * 0.9, 1),
                        'priority': 'medium'
                    })
            
            # Sort recommendations by priority
            priority_order = {'high': 0, 'medium': 1, 'low': 2}
            recommendations = sorted(recommendations, key=lambda x: priority_order.get(x['priority'], 3))
            
            logger.info(f"Generated {len(recommendations)} recommendations")
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            return []
    
    def run_full_pipeline(self, calendar_days_back=90, journal_path=None):
        """Run the full ETL pipeline with all components.
        
        Args:
            calendar_days_back: Number of days to fetch from calendar
            journal_path: Path to DayOne journal export
            
        Returns:
            dict: Pipeline results
        """
        results = {
            'success': False,
            'steps_completed': [],
            'steps_failed': [],
            'recommendations': [],
            'insights': {}
        }
        
        try:
            logger.info("Starting full ETL pipeline run")
            
            # Step 1: Google Calendar import
            calendar_success = self.run_google_calendar_import(days_back=calendar_days_back)
            if calendar_success:
                results['steps_completed'].append('google_calendar')
            else:
                results['steps_failed'].append('google_calendar')
            
            # Step 2: DayOne journal import (if path provided)
            journal_success = False
            if journal_path:
                journal_success = self.run_dayone_journal_import(journal_path)
                if journal_success:
                    results['steps_completed'].append('dayone_journal')
                else:
                    results['steps_failed'].append('dayone_journal')
            
            # Step 3: Calculate daily metrics
            metrics_success = self.calculate_daily_metrics()
            if metrics_success:
                results['steps_completed'].append('daily_metrics')
            else:
                results['steps_failed'].append('daily_metrics')
            
            # Step 4: Analyze patterns
            insights = self.analyze_patterns()
            if insights:
                results['steps_completed'].append('pattern_analysis')
                results['insights'] = insights
            else:
                results['steps_failed'].append('pattern_analysis')
            
            # Step 5: Generate recommendations
            recommendations = self.generate_recommendations()
            if recommendations:
                results['steps_completed'].append('recommendations')
                results['recommendations'] = recommendations
            else:
                results['steps_failed'].append('recommendations')
            
            # Overall success if at least calendar import worked
            results['success'] = 'google_calendar' in results['steps_completed']
            
            logger.info(f"Pipeline run completed. Steps completed: {results['steps_completed']}, Steps failed: {results['steps_failed']}")
            return results
            
        except Exception as e:
            logger.error(f"Error in pipeline run: {e}")
            results['error'] = str(e)
            return results

# Script entry point
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run the PPM data pipeline')
    parser.add_argument('--db', default='ppm.db', help='Path to SQLite database')
    parser.add_argument('--calendar-days', type=int, default=90, help='Number of days to fetch from Google Calendar')
    parser.add_argument('--journal-path', help='Path to DayOne journal export')
    parser.add_argument('--report', action='store_true', help='Generate a report after pipeline run')
    
    args = parser.parse_args()
    
    pipeline = DataPipeline(db_path=args.db)
    results = pipeline.run_full_pipeline(
        calendar_days_back=args.calendar_days,
        journal_path=args.journal_path
    )
    
    print(f"Pipeline completed: {results['success']}")
    print(f"Steps completed: {', '.join(results['steps_completed'])}")
    
    if results['steps_failed']:
        print(f"Steps failed: {', '.join(results['steps_failed'])}")
    
    if args.report and results['recommendations']:
        print("\n=== Recommendations ===")
        for rec in results['recommendations'][:5]:  # Show top 5
            print(f"{rec['priority'].upper()}: {rec['action']} {rec['domain']} " +
                  f"(currently {rec['current_percentage']}%, suggested {rec['suggested_percentage']}%)")
            print(f"  Reason: {rec['reason']}")
            print()