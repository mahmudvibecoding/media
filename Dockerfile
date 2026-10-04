FROM golang:1.26.8-bookworm AS proxy-build
WORKDIR /build/proxy-tester
COPY proxy-tester/go.mod proxy-tester/go.sum ./
RUN go mod download
COPY proxy-tester/*.go ./
RUN go test -race -count=1 -timeout=90s ./... \
    && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/proxy-tester .

FROM python:3.14.8-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MEDIA_STATE_DIR=/var/lib/media/state \
    MEDIA_OUTPUT_DIR=/var/lib/media/outputs \
    MEDIA_PROXY_BRIDGE_BINARY=/usr/local/bin/proxy-tester
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates openssl openssh-client rsync \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 media \
    && useradd --uid 10001 --gid media --create-home media \
    && mkdir -p /var/lib/media/state /var/lib/media/outputs \
    && chown -R media:media /var/lib/media
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=proxy-build /out/proxy-tester /usr/local/bin/proxy-tester
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
