"""Create private login configuration and a narrowly scoped database reader."""
import argparse
import os
from pathlib import Path
import secrets

from dashboard.security import password_hash


ROLE_COMMENT = 'Created for the Media Library dashboard'


def write_private(path, contents):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w') as output:
        output.write(contents)


def configure(folder):
    folder = Path(folder).resolve()
    configuration = folder / '.env.dashboard'
    if configuration.exists():
        print('Existing .env.dashboard preserved.')
        return
    password = secrets.token_urlsafe(24)
    values = dict(MEDIA_DASHBOARD_USERNAME='mahmud', MEDIA_DASHBOARD_PASSWORD_HASH=password_hash(password),
                  MEDIA_DASHBOARD_SECRET=secrets.token_urlsafe(48), MEDIA_DASHBOARD_DB_PASSWORD=secrets.token_urlsafe(36),
                  MEDIA_DASHBOARD_PORT='8050', MEDIA_DASHBOARD_SECURE_COOKIES='0')
    access = folder / '.local' / 'dashboard-access.txt'
    access.parent.mkdir(parents=True, exist_ok=True)
    if access.exists():
        raise RuntimeError('An access file already exists; recover the matching .env.dashboard before setup.')
    # Single quotes keep Compose from interpreting the dollar signs in the hash.
    write_private(configuration, ''.join(f"{key}='{value}'\n" for key, value in values.items()))
    write_private(access, f'Media Library\nURL: http://127.0.0.1:8050\nUsername: mahmud\nPassword: {password}\n')
    print('Private credentials saved to .local/dashboard-access.txt')


def grant():
    from psycopg import sql
    from runtime_config import connect_database
    password = os.environ.get('MEDIA_DASHBOARD_DB_PASSWORD', '')
    if len(password) < 32:
        raise RuntimeError('Load .env.dashboard before granting database access.')
    with connect_database('media') as conn:
        existing = conn.execute("SELECT shobj_description(oid,'pg_authid') FROM pg_roles WHERE rolname='media_dashboard'").fetchone()
        if existing and existing[0] != ROLE_COMMENT:
            raise RuntimeError('The media_dashboard role exists with an unrecognized owner; no permissions changed.')
        if not existing:
            conn.execute('CREATE ROLE media_dashboard LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION CONNECTION LIMIT 12')
            conn.execute(sql.SQL('COMMENT ON ROLE media_dashboard IS {}').format(sql.Literal(ROLE_COMMENT)))
        conn.execute(sql.SQL('ALTER ROLE media_dashboard PASSWORD {}').format(sql.Literal(password)))
        conn.execute('GRANT CONNECT ON DATABASE media TO media_dashboard')
        conn.execute('GRANT USAGE ON SCHEMA public TO media_dashboard')
        conn.execute('REVOKE ALL ON public.channels,public.videos,public.comments FROM media_dashboard')
        conn.execute('GRANT SELECT ON public.channels,public.videos,public.comments TO media_dashboard')
        conn.execute('ALTER ROLE media_dashboard IN DATABASE media SET default_transaction_read_only=on')
        conn.execute("ALTER ROLE media_dashboard IN DATABASE media SET statement_timeout='10s'")
    print('Dashboard reader has SELECT access to channels, videos, and comments.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('configure', 'grant'))
    parser.add_argument('--folder', default='.')
    args = parser.parse_args()
    configure(args.folder) if args.command == 'configure' else grant()
