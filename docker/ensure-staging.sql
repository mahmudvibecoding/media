SELECT 'CREATE DATABASE proxy_stage OWNER media'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='proxy_stage')
\gexec
