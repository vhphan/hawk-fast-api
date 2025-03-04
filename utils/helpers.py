import asyncio
import os

import fcntl
from dotenv import load_dotenv
from loguru import logger


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

async def run(cmd, working_dir=None):
    cwd = os.getcwd()
    if working_dir:
        os.chdir(working_dir)
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        print(f'{line.decode().strip()} [stdout]')

    while True:
        line = await proc.stderr.readline()
        if not line:
            break
        print(f'{line.decode().strip()} [stderr]')

    await proc.wait()
    print(f'[{cmd!r} exited with {proc.returncode}]')
    # stdout, stderr = await proc.communicate()
    #
    # print(f'[{cmd!r} exited with {proc.returncode}]')
    # if stdout:
    #     print(f'[stdout]\n{stdout.decode()}')
    # if stderr:
    #     print(f'[stderr]\n{stderr.decode()}')
    if working_dir:
        os.chdir(cwd)


async def run_python_module(module_path, python_path, working_dir, callback=None):
    logger.info(f'Running python module: {module_path}')
    cur_dir = os.getcwd()
    os.chdir(working_dir)
    process = await asyncio.create_subprocess_exec(python_path, '-m', module_path)
    await process.wait()
    os.chdir(cur_dir)
    logger.info(f'Finished running python module: {module_path}')
    if callback:
        callback()
    return process
