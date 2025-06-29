import mysql.connector
from mysql.connector import errorcode
import logging
import os
import hashlib

class MySQLHandler:
    def __init__(self, host, user, password, database):
        try:
            self.conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                use_pure=True
            )
            self.cursor = self.conn.cursor()
            logging.info("MySQL connection established successfully")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.error("Database does not exist. Please create it first.")
            else:
                logging.error(f"Database connection failed: {err}")
            raise

    def create_tables(self):
        """Create database tables from SQL script"""
        try:
            # Get absolute path to SQL file
            base_dir = os.path.dirname(os.path.abspath(__file__))
            sql_path = os.path.join(base_dir, "sql", "create_tables.sql")
            
            if not os.path.exists(sql_path):
                raise FileNotFoundError(f"SQL file not found: {sql_path}")
            
            logging.info(f"Creating tables from: {sql_path}")
            with open(sql_path, 'r') as f:
                sql_script = f.read()
                
            # Execute each statement separately
            for statement in sql_script.split(';'):
                stmt = statement.strip()
                if stmt:
                    try:
                        self.cursor.execute(stmt)
                    except mysql.connector.Error as err:
                        # Ignore "table already exists" errors
                        if err.errno != errorcode.ER_TABLE_EXISTS_ERROR:
                            raise
                    
            self.conn.commit()
            logging.info("Database tables created successfully")
            
        except Exception as e:
            logging.error(f"Error creating tables: {e}")
            raise

    def insert_batch_log_entries(self, entries):
        """Insert a batch of log entries with user agent normalization"""
        try:
            if not entries:
                return
                
            logging.info(f"Inserting batch of {len(entries)} log entries")
            user_agents_map = {}
            
            # First pass: handle user agent normalization
            for entry in entries:
                ua = entry["user_agent"]
                if ua not in user_agents_map:
                    self.cursor.execute("SELECT id FROM user_agents WHERE user_agent_string=%s", (ua,))
                    row = self.cursor.fetchone()
                    if row:
                        user_agents_map[ua] = row[0]
                    else:
                        self.cursor.execute(
                            "INSERT INTO user_agents (user_agent_string) VALUES (%s)", (ua,)
                        )
                        self.conn.commit()
                        user_agents_map[ua] = self.cursor.lastrowid
                        logging.debug(f"Added new user agent: {ua} (ID: {user_agents_map[ua]})")

            # Prepare log data
            log_data = [
                (
                    e["ip_address"], 
                    e["timestamp"], 
                    e["method"], 
                    e["path"], 
                    e["status_code"],
                    e["bytes_sent"], 
                    e["referrer"] if e["referrer"] != '-' else None,
                    user_agents_map[e["user_agent"]]
                )
                for e in entries
            ]

            # Batch insert log entries
            self.cursor.executemany("""
                INSERT INTO log_entries
                (ip_address, timestamp, method, path, status_code, bytes_sent, referrer, user_agent_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, log_data)
            self.conn.commit()
            logging.info(f"Inserted {len(entries)} log entries successfully")
            
        except Exception as e:
            logging.error(f"Error inserting log entries: {e}")
            self.conn.rollback()
            raise

    def get_top_n_ips(self, n):
        """Get top N IP addresses by request count"""
        try:
            self.cursor.execute("""
                SELECT ip_address, COUNT(*) as request_count
                FROM log_entries
                GROUP BY ip_address
                ORDER BY request_count DESC
                LIMIT %s
            """, (n,))
            return self.cursor.fetchall()
        except Exception as e:
            logging.error(f"Error fetching top IPs: {e}")
            return []

    def get_status_code_distribution(self):
        """Get status code distribution with percentages"""
        try:
            self.cursor.execute("""
                SELECT status_code, COUNT(*) AS count,
                (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM log_entries)) AS percentage
                FROM log_entries
                GROUP BY status_code
                ORDER BY count DESC
            """)
            return self.cursor.fetchall()
        except Exception as e:
            logging.error(f"Error fetching status distribution: {e}")
            return []

    def get_hourly_traffic(self):
        """Get request distribution by hour"""
        try:
            self.cursor.execute("""
                SELECT DATE_FORMAT(timestamp, '%H:00') AS hour, COUNT(*) AS requests
                FROM log_entries
                GROUP BY hour
                ORDER BY hour
            """)
            return self.cursor.fetchall()
        except Exception as e:
            logging.error(f"Error fetching hourly traffic: {e}")
            return []

    def close(self):
        """Close database connection"""
        try:
            self.cursor.close()
            self.conn.close()
            logging.info("Database connection closed")
        except Exception as e:
            logging.warning(f"Error closing database connection: {e}")