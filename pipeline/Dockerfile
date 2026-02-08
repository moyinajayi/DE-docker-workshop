FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

# Copy pyproject, python version, and lockfile from pipeline/ build context.
COPY pyproject.toml .python-version uv.lock ./
RUN uv sync

COPY ingest_data.py .

ENTRYPOINT ["python", "ingest_data.py"]

