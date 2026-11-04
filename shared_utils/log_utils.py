# log_utils.py
import logging
import os
from colorama import init, Fore, Style

# Init colorama
init(autoreset=True)

# === Create logger ===
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Accept all levels: DEBUG and above

# === Formatter (χωρίς χρώμα για αρχείο, με χρώμα για κονσόλα) ===
formatter = logging.Formatter(
    '[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# === File handler ===
log_path = os.path.join(os.getcwd(), "logfile.log")
file_handler = logging.FileHandler(log_path, encoding="utf-8")

file_handler.setLevel(logging.DEBUG)  # Save everything
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# === Console handler (με χρώματα) ===
class ColorFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        if record.levelno == logging.INFO:
            return Fore.CYAN + msg
        elif record.levelno == logging.DEBUG:
            return Fore.MAGENTA + msg
        elif record.levelno == logging.WARNING:
            return Fore.YELLOW + msg
        elif record.levelno == logging.ERROR:
            return Fore.RED + Style.BRIGHT + msg
        elif record.levelno == logging.CRITICAL:
            return Fore.RED + Style.BRIGHT + "[CRITICAL] " + msg
        return msg

console_handler = logging.StreamHandler()
console_handler.stream.reconfigure(encoding="utf-8") 
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(ColorFormatter(formatter._fmt, formatter.datefmt))
logger.addHandler(console_handler)

# === Shorthand functions ===
def info(msg):    logger.info(msg)
def success(msg): logger.info(Fore.GREEN + Style.BRIGHT + msg)
def warning(msg): logger.warning(msg)
def error(msg):   logger.error(msg)
def critical(msg): logger.critical(msg)
def debug(msg):   logger.debug(msg)
