FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN useradd -r -u 10001 neowave && chown -R neowave:neowave /app
USER neowave
ENV NEOWAVE_HOST=0.0.0.0 NEOWAVE_PORT=8080 NEOWAVE_DATA_DIR=/app/data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD python scripts/healthcheck.py http://127.0.0.1:8080/api/health || exit 1
CMD ["python","server.py"]
