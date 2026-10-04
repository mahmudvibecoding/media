FROM golang:1.26.8-bookworm AS proxy-build
WORKDIR /build/proxy-tester
COPY proxy-tester/go.mod proxy-tester/go.sum ./
RUN go mod download
COPY proxy-tester/*.go ./
RUN go test -race -count=1 -timeout=90s ./... \
    && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/proxy-tester .

FROM postgres:18.6-bookworm AS postgres-client
RUN mkdir -p /client \
    && cp /usr/lib/postgresql/18/bin/pg_restore /usr/lib/postgresql/18/bin/pg_dump /client/ \
    && cp -L "$(ldd /usr/lib/postgresql/18/bin/pg_restore | awk '$1 == "libpq.so.5" {print $3}')" /client/libpq.so.5

FROM python:3.14.8-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MEDIA_STATE_DIR=/var/lib/media/state \
    MEDIA_OUTPUT_DIR=/var/lib/media/outputs \
    MEDIA_PROXY_BRIDGE_BINARY=/usr/local/bin/proxy-tester
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates openssl openssh-client rsync libpq5 liblz4-1 libzstd1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 media \
    && useradd --uid 10001 --gid media --create-home media \
    && mkdir -p /var/lib/media/state /var/lib/media/outputs \
    && chown -R media:media /var/lib/media
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=proxy-build /out/proxy-tester /usr/local/bin/proxy-tester
COPY --from=postgres-client /client/ /usr/local/lib/media-postgres/
COPY --chmod=755 docker/pg_restore.sh /usr/local/bin/pg_restore
COPY --chmod=755 docker/pg_restore.sh /usr/local/bin/pg_dump
RUN pg_restore --version && pg_dump --version
COPY *.py ./
COPY db/ ./db/
COPY data/channels.csv ./data/channels.csv
COPY proxy-tester/*.py ./proxy-tester/
COPY tests/ ./tests/
COPY scripts/ ./scripts/
COPY docker/ ./docker/
USER media
ENTRYPOINT ["sh", "/app/docker/entrypoint.sh"]
CMD ["python", "manage.py", "status"]
