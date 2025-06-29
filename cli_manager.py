import argparse
from log_parser import LogParser
from tabulate import tabulate
import logging
import os

class CLIManager:
    def __init__(self, db_handler):
        self.db_handler = db_handler
        self.parser = argparse.ArgumentParser(description="Web Server Log Analyzer & Reporting CLI")
        self._setup_parser()
        logging.info("CLI Manager initialized")

    def _setup_parser(self):
        subparsers = self.parser.add_subparsers(dest='command', help='Available commands')
        
        # Process logs command
        process_parser = subparsers.add_parser('process_logs', help='Parse and load logs from a file.')
        process_parser.add_argument('file_path', type=str, help='Path to the log file.')
        process_parser.add_argument('--batch_size', type=int, default=1000, help='Batch size for DB inserts.')
        
        # Generate report command
        report_parser = subparsers.add_parser('generate_report', help='Generate various analytical reports.')
        report_subparsers = report_parser.add_subparsers(dest='report_type', help='Types of reports')
        
        # Report types
        top_ips_parser = report_subparsers.add_parser('top_n_ips', help='Top N requesting IP addresses.')
        top_ips_parser.add_argument('n', type=int, help='Number of top IPs.')
        
        report_subparsers.add_parser('status_code_distribution', help='HTTP status code breakdown.')
        report_subparsers.add_parser('hourly_traffic', help='Traffic distribution by hour.')

    def run(self):
        args = self.parser.parse_args()
        
        if not args.command:
            self.parser.print_help()
            return
            
        if args.command == 'process_logs':
            self._process_logs(args)
        elif args.command == 'generate_report':
            self._generate_report(args)
        else:
            self.parser.print_help()

    def _process_logs(self, args):
        """Process log files and insert into database"""
        try:
            # Resolve log file path
            file_path = self._resolve_file_path(args.file_path)
            if not file_path:
                return
                
            parser = LogParser()
            processed = 0
            batch = []
            
            logging.info(f"Processing log file: {file_path}")
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        parsed = parser.parse_line(line)
                        if parsed:
                            batch.append(parsed)
                            
                        if len(batch) >= args.batch_size:
                            self.db_handler.insert_batch_log_entries(batch)
                            processed += len(batch)
                            batch.clear()
                            logging.info(f"Processed {line_num} lines...")
                            
                    except Exception as e:
                        logging.warning(f"Error processing line {line_num}: {e}")
                        
                # Insert remaining entries
                if batch:
                    self.db_handler.insert_batch_log_entries(batch)
                    processed += len(batch)
                    
            logging.info(f"Finished processing log file. Total valid entries loaded: {processed}")
            
        except Exception as e:
            logging.error(f"Fatal error processing logs: {e}")
            raise

    def _generate_report(self, args):
        """Generate requested reports"""
        try:
            if not args.report_type:
                self.parser.print_help()
                return
                
            if args.report_type == 'top_n_ips':
                results = self.db_handler.get_top_n_ips(args.n)
                print("\nTop Requesting IP Addresses:")
                print(tabulate(results, headers=["IP Address", "Request Count"], tablefmt="grid"))
                
            elif args.report_type == 'status_code_distribution':
                results = self.db_handler.get_status_code_distribution()
                print("\nHTTP Status Code Distribution:")
                # Format percentage with % sign
                formatted_results = [(code, count, f"{percentage:.2f}%") 
                                    for code, count, percentage in results]
                print(tabulate(formatted_results, headers=["Status Code", "Count", "Percentage"], tablefmt="grid"))
                
            elif args.report_type == 'hourly_traffic':
                results = self.db_handler.get_hourly_traffic()
                print("\nHourly Traffic Distribution:")
                print(tabulate(results, headers=["Hour", "Requests"], tablefmt="grid"))
                
            else:
                self.parser.print_help()
                
        except Exception as e:
            logging.error(f"Error generating report: {e}")
            raise

    def _resolve_file_path(self, file_path):
        """Resolve file path with fallback to sample_logs directory"""
        if os.path.exists(file_path):
            return file_path
            
        # Try looking in sample_logs folder
        sample_path = os.path.join("sample_logs", file_path)
        if os.path.exists(sample_path):
            return sample_path
            
        logging.error(f"Log file not found: {file_path}")
        logging.info("Checked locations:")
        logging.info(f"- {os.path.abspath(file_path)}")
        logging.info(f"- {os.path.abspath(sample_path)}")
        return None