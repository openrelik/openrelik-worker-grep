# Openrelik worker for running grep.
This worker support running grep against input files and mounted disk images.

### Installation
Add the below configuration to the OpenRelik `docker-compose.yml` file.

```
openrelik-worker-grep:
    container_name: openrelik-worker-grep
    image: ghcr.io/openrelik/openrelik-worker-grep:${OPENRELIK_WORKER_GREP_VERSION}
    privileged: true
    restart: always
    environment:
      - REDIS_URL=redis://openrelik-redis:6379
    volumes:
      - /dev:/dev
      - ./data:/usr/share/openrelik/data
    command: "celery --app=src.app worker --task-events --concurrency=4 --loglevel=INFO -Q openrelik-worker-grep"
```

## Tests
```
uv sync --group test
uv run pytest -s --cov=.
```