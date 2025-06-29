import os
import sys
import argparse
from cli_manager import CLIManager
from mysql_handler import MySQLHandler
import configparser
import logging

# Force pure Python MySQL connector
os.environ["MYSQL_CONNECTOR_USE_PURE"] = "true"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    try:
        config = configparser.ConfigParser()
        
        # Handle config file path
        config_path = "config.ini"
        if not os.path.exists(config_path):
            logging.error(f"Config file not found: {config_path}")
            sys.exit(1)
            
        config.read(config_path)

        db_config = {
            "host": config.get("mysql", "host"),
            "user": config.get("mysql", "user"),
            "password": config.get("mysql", "password"),
            "database": config.get("mysql", "database")
        }

        db_handler = MySQLHandler(**db_config)
        logging.info("Database connection established")
        
        db_handler.create_tables()
        cli = CLIManager(db_handler)
        cli.run()
        db_handler.close()
        logging.info("Application completed successfully")
        
    except Exception as e:
        logging.exception("Fatal error occurred:")
        sys.exit(1)

if __name__ == "__main__":
    main()