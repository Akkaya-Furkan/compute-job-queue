FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
COPY compute_queue ./compute_queue
RUN pip install --no-cache-dir . && useradd --uid 10001 --create-home queue && mkdir /data && chown queue:queue /data
USER 10001
VOLUME ["/data"]
EXPOSE 8000
CMD ["compute-queue", "--host", "0.0.0.0", "--db", "/data/queue.sqlite", "--workers", "2"]
