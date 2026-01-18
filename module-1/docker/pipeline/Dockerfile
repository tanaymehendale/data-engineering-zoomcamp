# Start with base image as a template -- slim Python 3.13 for smaller size
FROM python:3.13.11-slim

# COPY from different Docker image (official uv image) to a directory inside the container
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code

# Add virtual environment to PATH
ENV PATH="/code/.venv/bin:$PATH"

# Copy dependenciy files first (better caching)
COPY pyproject.toml .python-version uv.lock ./

# Install dependencies from uv.lock file - 
# we make sure that dependencies installed in container are the same as we have in virtualenv
# ensures reproducble builds
RUN uv sync --locked


# COPY from host machine to a directory inside the container
# COPY pipeline.py . 
# Copy the ingestion script (ingest_data.py) to current directory (.)
COPY ingest_data.py .

# ENTRYPOINT ["uv", "run", "python", "pipeline.py"] -- to not write uv run here, use the ENV PATH
ENTRYPOINT ["python", "ingest_data.py"]