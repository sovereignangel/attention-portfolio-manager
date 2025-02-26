import sqlite3

def create_database():
    conn = sqlite3.connect('ppm.db')
    c = conn.cursor()
    
    # Activities table
    c.execute('''
    CREATE TABLE IF NOT EXISTS activities (
        activity_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        domain TEXT NOT NULL,  
        energy_impact INTEGER,  
        engagement_level INTEGER,
        time_horizon TEXT,
        input_output_ratio TEXT,
        challenge_level INTEGER,
        social_dimension TEXT,
        intention_level INTEGER,
        value_alignment INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Time entries table
    c.execute('''
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
    
    # Outcome metrics table
    c.execute('''
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
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_database()