# Log Analyzer CLI

Web server log analysis and reporting system that processes Apache-style logs, stores data in MySQL, and generates analytical reports.

## Features
- Log parsing and ingestion
- MySQL database storage
- Analytical reporting (top IPs, status codes, hourly traffic)
- Batch processing for large files

## Setup
1. Install requirements:
   pip install -r requirements.txt
2. Configure MySQL in `config.ini`
3. Create tables:  [project 2 test results.docx](https://github.com/user-attachments/files/20970212/project.2.test.results.docx)
   python main.py
4. Process logs:
   python main.py process_logs sample_logs/access.log
5. Generate reports:
   python main.py generate_report top_n_ips 5
6. Status Distribution:
   python main.py generate_report status_code_distribution
7. Hourly Traffic:
   python main.py generate_report hourly_traffic

## Project Structure
├── .gitignore
├── main.py
├── log_parser.py
├── cli_manager.py
├── mysql_handler.py
├── requirements.txt
├── config.ini
├── sql/
│   └── create_tables.sql
└── sample_logs/
    └── access.log
