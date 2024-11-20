import os

import fcntl
from dotenv import load_dotenv


def get_env(key: str) -> str:
    env_value = os.getenv(key)
    if env_value is None:
        load_dotenv()  # Retry loading environment variables
        env_value = os.getenv(key)
        if env_value is None:
            raise ValueError(f"Environment variable {key} not set.")
    return env_value


def lock_file(file_path):
    fd = os.open(file_path, os.O_RDWR | os.O_CREAT)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print(f"File {file_path} locked successfully.")
        return fd
    except BlockingIOError:
        os.close(fd)
        print(f"File {file_path} is already locked.")
        return None


def unlock_file(fd):
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)
    print("File unlocked successfully.")

# # Example usage
# file_path = 'tmp/master.lock'
# try:
#     fd = lock_file(file_path)
#     # Perform operations with the locked file
# finally:
#     unlock_file(fd)
