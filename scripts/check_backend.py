"""Run Go and Python tests with a private, disposable PostgreSQL cluster.

Usage: .venv/bin/python scripts/check_backend.py [--postgres-bin /path/to/bin]
Requires PostgreSQL 18 server tools, Go 1.26, OpenSSL, and requirements.txt.
"""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]


def run(command, **kwargs):
    return subprocess.run([str(arg) for arg in command], check=True, **kwargs)


def postgres_bin(explicit):
    if explicit:
        folder = explicit.resolve()
    elif shutil.which("pg_config"):
        folder = Path(run(["pg_config", "--bindir"], capture_output=True, text=True).stdout.strip())
    else:
        raise ValueError("PostgreSQL tools not found; pass --postgres-bin with their bin directory")
    for name in ("initdb", "pg_ctl", "createdb", "psql"):
        if not (folder / name).is_file():
            raise ValueError(f"Missing {folder / name}; install PostgreSQL server tools")
    return folder


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-bin", type=Path)
    args = parser.parse_args()
    try:
        pg = postgres_bin(args.postgres_bin)
        for command in ("go", "openssl"):
            if not shutil.which(command):
                raise ValueError(f"Missing required command: {command}")
    except ValueError as exc:
        parser.error(str(exc))
    if os.geteuid() == 0:
        parser.error("Run as a normal user; PostgreSQL initdb does not run as root")

    # Keep the Unix socket path short enough for macOS as well as Linux.
    folder = Path(tempfile.mkdtemp(prefix="media-check-", dir="/tmp"))
    data, sock = folder / "data", folder / "socket"
    sock.mkdir(mode=0o700)
    log = folder / "postgres.log"
    # Inherited PostgreSQL settings must not redirect these tests elsewhere.
    env = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
    connection = ["-h", str(sock), "-p", "5432", "-U", "media_test"]
    try:
        print("Checking Go transports and building the collection bridge...", flush=True)
        run(["go", "test", "-race", "-count=1", "-timeout=90s", "./..."], cwd=ROOT / "proxy-tester")
        (ROOT / ".local/bin").mkdir(parents=True, exist_ok=True)
        run(["go", "build", "-trimpath", "-o", ROOT / ".local/bin/proxy-tester", "."],
            cwd=ROOT / "proxy-tester")

        print("Creating isolated media and proxy test databases...", flush=True)
        run([pg / "initdb", "-D", data, "-A", "trust", "-U", "media_test", "--no-locale", "-E", "UTF8"],
            env=env, stdout=subprocess.DEVNULL)
        run([pg / "pg_ctl", "-D", data, "-l", log, "-o",
             f"-c listen_addresses='' -c unix_socket_directories='{sock}' -p 5432", "-w", "start"],
            env=env, timeout=45)
        for database, schema in (("media", "db/schema.sql"), ("proxy", "db/proxy/schema.sql")):
            run([pg / "createdb", *connection, database], env=env)
            run([pg / "psql", "-X", *connection, "-d", database, "-v", "ON_ERROR_STOP=1",
                 "-f", ROOT / schema], env=env, stdout=subprocess.DEVNULL)
        run([pg / "psql", "-X", *connection, "-d", "media", "-v", "ON_ERROR_STOP=1",
             "-c", "CREATE ROLE media_viewer"], env=env, stdout=subprocess.DEVNULL)
        for database in ("media", "proxy"):
            env[f"{database.upper()}_TEST_DATABASE_URL"] = (
                f"host={sock} port=5432 user=media_test dbname={database} connect_timeout=5")
        env["PROXY_TEST_DATABASE"] = "1"
        # Test collectors use mocked HTTP or local fixture servers.
        print("Running all Python tests, including database and bridge checks...", flush=True)
        run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"], cwd=ROOT, env=env)
    except BaseException:
        if log.exists():
            print("PostgreSQL log tail:\n" + "\n".join(log.read_text().splitlines()[-25:]), file=sys.stderr)
        raise
    finally:
        stopped = True
        if (data / "postmaster.pid").exists():
            result = subprocess.run([str(pg / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"],
                                    env=env, timeout=45)
            stopped = result.returncode == 0
        if stopped:
            shutil.rmtree(folder)
        else:
            raise RuntimeError(f"Test database did not stop; retained its files at {folder}")
    print("Backend checks passed; the temporary PostgreSQL cluster was removed.", flush=True)


if __name__ == "__main__":
    main()
