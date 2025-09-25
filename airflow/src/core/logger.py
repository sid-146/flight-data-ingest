import logging
from datetime import datetime
import os


# Logs file Logging object
logger = logging.getLogger("logs")
logger.setLevel(logging.INFO)


# Console + Log File Logging object
console = logging.getLogger("console")
console.setLevel(logging.INFO)

# console streaming handler
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.INFO)

# if not os.path.exists("./logs"):
#     os.makedirs("./logs", exist_ok=True)

# Log File handler
# f_handler = logging.FileHandler(
#     f"./logs/log_{datetime.now().strftime('%Y_%m_%d')}.log", mode="a"
# )
# f_handler.setLevel(logging.INFO)

# Adding output formatter
c_format = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - : %(message)s"
)
# f_format = logging.Formatter(
#     "%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - : %(message)s"
# )


c_handler.setFormatter(c_format)
# f_handler.setFormatter(f_format)


# Setting up logging objects
console.addHandler(c_handler)
# console.addHandler(f_handler)

# Added file handler
# logs.addHandler(f_handler)
