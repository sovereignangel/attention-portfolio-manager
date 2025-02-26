from __future__ import print_function
import datetime as dt
import os.path
import pickle
import json
import time
import pandas as pd
from datetime import timedelta, datetime
import plotly.express as px
import plotly.graph_objects as go
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import streamlit as st

# Define the scopes
SCOPES = [
    'https://www.googleapis.com/auth/calendar.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file'
]

# Authenticate Google Calendar and Google Sheets
def authenticate_gcal():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds_info = json.load(token)
            creds = Credentials.from_authorized_user_info(creds_info, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Error refreshing credentials: {e}")
                os.remove('token.pickle')
                creds = None
        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(
    'docs/credentials.json', SCOPES)  # Specify the path to docs folder
            creds = flow.run_local_server(port=0)
            with open('token.pickle', 'w') as token:
                json.dump(json.loads(creds.to_json()), token)
    return creds

# Fetch Calendar Events for the specified number of days back
def get_calendar_events(days_back=30):
    creds = authenticate_gcal()
    service = build('calendar', 'v3', credentials=creds)
    
    # Calculate time window
    now = dt.datetime.utcnow()
    time_min = (now - timedelta(days=days_back)).isoformat() + 'Z'
    time_max = (now - timedelta(days=1)).isoformat() + 'Z'  # Only up to yesterday
    
    print(f'Getting events from {days_back} days ago to yesterday')
    events_result = service.events().list(calendarId='primary', timeMin=time_min, timeMax=time_max,
                                          singleEvents=True, orderBy='startTime').execute()
    
    events = events_result.get('items', [])
    if not events:
        print('No events found.')
        return []
    
    event_list = []
    event_id_counter = 1
    
    for event in events:
        start = event['start'].get('dateTime', event['start'].get('date'))
        end = event['end'].get('dateTime', event['end'].get('date'))
        description = event.get('description', '')
        
        # Skip events without proper date formatting
        if not ('T' in start and 'T' in end):
            continue
            
        try:
            start_dt = dt.datetime.fromisoformat(start.replace('Z', '+00:00'))
            end_dt = dt.datetime.fromisoformat(end.replace('Z', '+00:00'))
            total_time = (end_dt - start_dt).total_seconds() / 3600
            
            identifier = f"{event_id_counter:05d}"
            date = start_dt.strftime('%Y-%m-%d')
            time_val = start_dt.hour + start_dt.minute / 60
            
            event_list.append([
                identifier, 
                date, 
                time_val, 
                event['summary'], 
                total_time, 
                description,
                start_dt.strftime('%H:%M'),
                end_dt.strftime('%H:%M')
            ])
            event_id_counter += 1
        except Exception as e:
            print(f"Error processing event {event.get('summary', 'Unknown')}: {e}")
    
    return event_list

# Enhanced categorization based on your metrics framework
def categorize_event(event_name, description):
    keywords = {
        "Generation": ["create", "build", "develop", "design", "work", "project", "produce", "write", "code", "draft", "edit", "make", "generate", "create", "brainstorm"],
        "Charging": ["rest", "relax", "recharge", "sleep", "meditate", "recovery", "break", "nap", "unwind", "breathe", "yoga", "self-care"],
        "Growth": ["learn", "study", "read", "course", "training", "development", "skill", "improve", "practice", "master", "workshop", "webinar"],
        "Connection": ["meet", "call", "chat", "coffee", "lunch", "dinner", "social", "friend", "family", "team", "collaborate", "network", "relationship"],
        "Vitality": ["exercise", "gym", "workout", "run", "walk", "hike", "swim", "health", "doctor", "nutrition", "fitness", "physical"]
    }
    
    # Check for matches in both event name and description
    text = (event_name + " " + description).lower()
    
    for category, terms in keywords.items():
        for term in terms:
            if term in text:
                return category, f"Matched keyword '{term}'"
    
    return "Other", "No matching keywords found"

# Calculate attention metrics
def calculate_attention(events):
    events_df = pd.DataFrame(events, columns=['Event ID', 'Date', 'Time', 'Event Name', 'Event Total Time', 'Description', 'Start Time', 'End Time'])
    events_df['Theme'] = events_df.apply(lambda row: categorize_event(row['Event Name'], row['Description'])[0], axis=1)
    
    # Group by date and theme to get time allocation
    attention_data = events_df.groupby(['Date', 'Theme'])['Event Total Time'].sum().reset_index()
    
    # Calculate total time per day
    total_time_per_day = events_df.groupby('Date')['Event Total Time'].sum().reset_index()
    total_time_per_day.columns = ['Date', 'Total Time']
    
    # Merge to calculate percentages
    attention_data = pd.merge(attention_data, total_time_per_day, on='Date')
    attention_data['Theme % Amount'] = (attention_data['Event Total Time'] / attention_data['Total Time']) * 100
    
    # Add timeframe column
    attention_data['Timeframe'] = 'day'
    
    # Rename columns for consistency
    attention_data = attention_data.rename(columns={'Event Total Time': 'Theme Absolute Amount'})
    
    return attention_data[['Date', 'Timeframe', 'Theme', 'Theme Absolute Amount', 'Theme % Amount']]

# Identify patterns and generate insights
def identify_patterns(events_df, attention_df):
    insights = []
    
    # 1. Time of day productivity patterns
    try:
        events_df['Time_Category'] = pd.cut(
            events_df['Time'], 
            bins=[0, 12, 17, 24], 
            labels=['Morning', 'Afternoon', 'Evening']
        )
        
        time_productivity = events_df[events_df['Theme'] == 'Generation'].groupby('Time_Category')['Event Total Time'].sum()
        
        if not time_productivity.empty:
            most_productive_time = time_productivity.idxmax()
            insights.append(f"You're most productive during {most_productive_time} hours. Consider scheduling high-value Generation activities during this time.")
    except Exception as e:
        print(f"Error in time productivity analysis: {e}")
    
    # 2. Balance analysis
    try:
        theme_balance = attention_df.groupby('Theme')['Theme Absolute Amount'].sum()
        total_time = theme_balance.sum()
        
        if total_time > 0:
            generation_pct = (theme_balance.get('Generation', 0) / total_time) * 100
            charging_pct = (theme_balance.get('Charging', 0) / total_time) * 100
            
            if generation_pct > 60:
                insights.append(f"You're spending {generation_pct:.1f}% of your time on Generation activities. Consider increasing Charging time to maintain energy levels.")
            
            if charging_pct < 20:
                insights.append(f"You're only spending {charging_pct:.1f}% of your time on Charging activities. This may lead to burnout. Try to allocate at least 20% for recovery.")
    except Exception as e:
        print(f"Error in balance analysis: {e}")
    
    # 3. Streak analysis
    try:
        events_df = events_df.sort_values(by=['Date', 'Time'])
        daily_generation = events_df[events_df['Theme'] == 'Generation'].groupby('Date')['Event Total Time'].sum()
        
        # Find streaks of consistent generation time
        streak_days = 0
        for day, value in daily_generation.items():
            if value > 2:  # More than 2 hours of generation
                streak_days += 1
            else:
                streak_days = 0
        
        if streak_days >= 3:
            insights.append(f"You've maintained {streak_days} consecutive days with significant Generation time. Great consistency!")
    except Exception as e:
        print(f"Error in streak analysis: {e}")
    
    # 4. Theme diversity
    try:
        unique_themes = attention_df['Theme'].nunique()
        if unique_themes <= 2:
            insights.append("Your attention portfolio lacks diversity. Consider allocating time to more domains for better balance.")
    except Exception as e:
        print(f"Error in theme diversity analysis: {e}")
    
    # Add default insight if none were generated
    if not insights:
        insights.append("Not enough data for meaningful patterns yet. Continue tracking your time for more insights.")
    
    return insights

# Generate recommendations based on insights and patterns
def generate_recommendations(events_df, attention_df, insights):
    recommendations = []
    
    # 1. Generation-Charging balance recommendation
    try:
        theme_totals = attention_df.groupby('Theme')['Theme Absolute Amount'].sum()
        generation_time = theme_totals.get('Generation', 0)
        charging_time = theme_totals.get('Charging', 0)
        
        g_to_c_ratio = generation_time / charging_time if charging_time > 0 else float('inf')
        
        if g_to_c_ratio > 3:
            recommendations.append("Action: Schedule a dedicated 30-minute Charging activity after every 3 hours of Generation work.")
    except Exception as e:
        print(f"Error in balance recommendation: {e}")
    
    # 2. Time of day optimization
    try:
        events_df['Hour'] = events_df['Time'].apply(lambda x: int(x))
        generation_by_hour = events_df[events_df['Theme'] == 'Generation'].groupby('Hour')['Event Total Time'].sum()
        
        if not generation_by_hour.empty:
            peak_hour = generation_by_hour.idxmax()
            recommendations.append(f"Action: Protect the {peak_hour}:00-{peak_hour+1}:00 hour as your 'peak performance time' for your most important Generation work.")
    except Exception as e:
        print(f"Error in time optimization recommendation: {e}")
    
    # 3. Theme gap recommendation
    try:
        missing_themes = set(['Generation', 'Charging', 'Growth', 'Connection', 'Vitality']) - set(attention_df['Theme'].unique())
        
        if missing_themes:
            missing_theme = next(iter(missing_themes))
            recommendations.append(f"Action: Add at least one {missing_theme} activity to your schedule this week.")
    except Exception as e:
        print(f"Error in theme gap recommendation: {e}")
    
    # Add default recommendation if none were generated
    if not recommendations:
        recommendations.append("Action: Begin each day by identifying your most important Generation task and schedule it during your peak energy hours.")
    
    return recommendations

# Create a Streamlit dashboard
def create_streamlit_dashboard(events_df, attention_df, insights, recommendations):
    st.set_page_config(page_title="Personal Portfolio Manager", layout="wide")
    
    st.title("Personal Portfolio Manager")
    st.subheader("Your Digital Twin for Life Optimization")
    
    st.write("---")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    total_hours = events_df['Event Total Time'].sum()
    unique_days = events_df['Date'].nunique()
    theme_count = attention_df['Theme'].nunique()
    avg_daily_hours = total_hours / unique_days if unique_days > 0 else 0
    
    col1.metric("Total Hours Tracked", f"{total_hours:.1f}")
    col2.metric("Days Analyzed", unique_days)
    col3.metric("Activity Domains", theme_count)
    col4.metric("Avg Daily Hours", f"{avg_daily_hours:.1f}")
    
    st.write("---")
    
    # Main content
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.subheader("Time Allocation by Domain")
        
        # Prepare data for pie chart
        theme_totals = attention_df.groupby('Theme')['Theme Absolute Amount'].sum().reset_index()
        
        # Create pie chart
        fig = px.pie(
            theme_totals, 
            values='Theme Absolute Amount', 
            names='Theme',
            color='Theme',
            color_discrete_map={
                'Generation': '#2E86C1',
                'Charging': '#27AE60',
                'Growth': '#F1C40F',
                'Connection': '#E67E22',
                'Vitality': '#8E44AD',
                'Other': '#7F8C8D'
            },
            title="Your Attention Portfolio"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Daily theme breakdown
        st.subheader("Daily Theme Distribution")
        daily_data = events_df.groupby(['Date', 'Theme'])['Event Total Time'].sum().reset_index()
        daily_data = daily_data.pivot(index='Date', columns='Theme', values='Event Total Time').fillna(0)
        
        # Create stacked bar chart
        fig = px.bar(
            daily_data, 
            barmode='stack',
            title="Daily Time Allocation"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Insights & Patterns")
        for i, insight in enumerate(insights):
            st.info(insight)
        
        st.subheader("Recommended Actions")
        for i, recommendation in enumerate(recommendations):
            st.success(recommendation)
        
        # Time of day analysis
        st.subheader("Productivity by Time of Day")
        try:
            hourly_data = events_df.groupby(['Hour', 'Theme'])['Event Total Time'].sum().reset_index()
            generation_hours = hourly_data[hourly_data['Theme'] == 'Generation']
            
            fig = px.bar(
                generation_hours, 
                x='Hour', 
                y='Event Total Time',
                title="Generation Activity by Hour"
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.write("Not enough hourly data yet.")
    
    st.write("---")
    
    # Raw data section (collapsible)
    with st.expander("View Raw Event Data"):
        st.dataframe(events_df[['Date', 'Event Name', 'Theme', 'Start Time', 'End Time', 'Event Total Time']])

# Main function
def main():
    # Get events from the last 14 days
    events = get_calendar_events(days_back=14)
    
    if events:
        # Create DataFrame from events
        events_df = pd.DataFrame(
            events, 
            columns=['Event ID', 'Date', 'Time', 'Event Name', 'Event Total Time', 'Description', 'Start Time', 'End Time']
        )
        
        # Add theme to events dataframe
        events_df['Theme'] = events_df.apply(lambda row: categorize_event(row['Event Name'], row['Description'])[0], axis=1)
        
        # Calculate attention metrics
        attention_df = calculate_attention(events)
        
        # Generate insights and recommendations
        insights = identify_patterns(events_df, attention_df)
        recommendations = generate_recommendations(events_df, attention_df, insights)
        
        # Create dashboard
        create_streamlit_dashboard(events_df, attention_df, insights, recommendations)
    else:
        st.title("Personal Portfolio Manager")
        st.error("No calendar events found for analysis. Please check your Google Calendar integration.")
        st.write("Make sure you have events in your calendar for the past 14 days.")

if __name__ == '__main__':
    main()