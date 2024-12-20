import subprocess


def check_tmux_session_exists(session_name: str, lock_name=None) -> bool:
    lock_params = f'-L {lock_name}' if lock_name else ''
    output = subprocess.run(f'tmux {lock_params} has-session -t ={session_name}',
                            shell=True,
                            capture_output=True,
                            text=True)
    return output.returncode == 0


def create_tmux_session(session_name: str, lock_name=None):
    lock_params = f'-L {lock_name}' if lock_name else ''
    result = subprocess.run(f'tmux {lock_params} new-session -d -s {session_name}', shell=True, check=True)
    return result


def get_tmux_windows(session_name: str, lock_name=None) -> bool:
    lock_params = f'-L {lock_name}' if lock_name else ''
    try:

        windows = subprocess.run(
            f'tmux {lock_params} list-windows -t ={session_name}' + ' -F "#{window_name}"',
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        windows_list = windows.stdout.decode().split('\n')
        windows_list = list(filter(None, windows_list))
        return windows_list
    except subprocess.CalledProcessError as e:
        # e.returncode is the exit code of the process and is 1 if the process returned an error
        # print(e.returncode)
        print(e.stderr.decode()) # This will print the error message from the process
        return []


if __name__ == '__main__':
    session_name = 'eutrancellfdd_flex'
    lock_name = 'pp'
    print(get_tmux_windows(session_name, lock_name))
