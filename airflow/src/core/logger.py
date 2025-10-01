import logging
import os
from datetime import datetime

level = logging.DEBUG


class ColoredFormatter(logging.Formatter):
    # ANSI escape codes for colors
    COLORS = {
        "DEBUG": "\033[94m",  # Blue
        "INFO": "\033[92m",  # Green
        "WARNING": "\033[93m",  # Yellow
        "ERROR": "\033[91m",  # Red
        "CRITICAL": "\033[95m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record):
        level_name = record.levelname
        if level_name in self.COLORS:
            # Add color to the levelname
            record.levelname = f"{self.COLORS[level_name]}{level_name}{self.RESET}"
        return super().format(record)


# Utility Code
def get_path():
    try:
        # return os.path.dirname(os.path.abspath("*.py"))
        return os.path.dirname(os.getcwd())
    except SystemError as e:
        print(f"Error while getting path for log file: {e}")


path = get_path()
repo = "flight-data-ingest"
filename = f"logs_{datetime.now().strftime('%Y_%m_%d_%H_%M')}.log"

current_dir = os.path.basename(path)

if current_dir == repo:
    path = os.path.join(path, "logs")
elif current_dir == "src":
    path = os.path.join(path, "logs")
else:
    path = os.path.join(path, repo, "logs")

# os.makedirs(path, exist_ok=True)

# filepath = os.path.join(path, filename)

"""
    Handlers
"""
c_handler = logging.StreamHandler()
# f_handler = logging.FileHandler(filename=filepath, mode="a")

"""
Formatter
"""
c_format = ColoredFormatter(
    "%(levelname)s: %(asctime)s : %(module)s : [%(funcName)s] : %(lineno)d : %(message)s"
)
f_format = logging.Formatter(
    "%(asctime)s : [%(levelname)s] : %(module)s : [%(funcName)s] : %(lineno)d : %(message)s"
)

# Setting formats for handler
c_handler.setFormatter(c_format)
c_handler.setLevel(level=level)
# f_handler.setFormatter(f_format)

"""
    Console Logger
"""
console = logging.getLogger("console")
console.setLevel(level=level)
console.addHandler(c_handler)
# console.addHandler(f_handler)

mail = console
# mail.addHandler(f_handler)
"""
    File Logger
"""
# (Assuming you have additional configuration here)


__all__ = ["console", "mail"]
