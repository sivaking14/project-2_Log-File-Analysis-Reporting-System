import re
from datetime import datetime
import logging

class LogParser:
    LOG_PATTERN = re.compile(
        r'(?P<ip>\d+\.\d+\.\d+\.\d+) - - \[(?P<time>.*?)\] "(?P<request>.*?)" (?P<status>\d{3}) (?P<bytes>\d+|-) "(?P<referrer>.*?)" "(?P<user_agent>.*?)"'
    )

    def parse_line(self, line):
        match = self.LOG_PATTERN.match(line)
        if match:
            try:
                parts = match.groupdict()
                request_parts = parts["request"].split()
                method = request_parts[0] if len(request_parts) > 0 else None
                path = request_parts[1] if len(request_parts) > 1 else None
                return {
                    "ip_address": parts["ip"],
                    "timestamp": datetime.strptime(parts["time"], "%d/%b/%Y:%H:%M:%S %z"),
                    "method": method,
                    "path": path,
                    "status_code": int(parts["status"]),
                    "bytes_sent": int(parts["bytes"]) if parts["bytes"].isdigit() else 0,
                    "referrer": parts["referrer"],
                    "user_agent": parts["user_agent"]
                }
            except Exception as e:
                logging.warning(f"Malformed log line skipped: {line.strip()}")
        return None
