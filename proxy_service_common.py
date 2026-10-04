"""Small durable-file helpers shared by catalog synchronization and testing."""
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess

from runtime_config import STATE_DIR


SERVICE_DIR = STATE_DIR / "proxy-service"


def available_memory():
    memory = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    cap = Path("/sys/fs/cgroup/memory.max")
    if cap.exists() and cap.read_text().strip() != "max":
        memory = min(memory, int(cap.read_text()))
    return memory


def utcnow():
    return datetime.now(timezone.utc)


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return default


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + ".partial")
    with temporary.open("w") as output:
        os.chmod(temporary, 0o600)
        json.dump(value, output, default=str, separators=(",", ":"))
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def file_lock(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def run_owned(command, *, stop=None, **kwargs):
    """Stop only this child's process group, including its restore workers."""
    with subprocess.Popen(command, start_new_session=True, **kwargs) as process:
        try:
            while True:
                try:
                    return process.wait(timeout=0.25)
                except subprocess.TimeoutExpired:
                    if stop is not None and stop.is_set():
                        raise InterruptedError("Service is stopping; checkpoint retained")
        except BaseException:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            raise
