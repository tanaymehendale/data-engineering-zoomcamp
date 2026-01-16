FROM python:3.13.11-slim

# COPY from different Docker image to a directory inside the container
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./

# Install dependencies from uv.lock file - 
# we make sure that dependencies installed in container are the same as we have in virtualenv
RUN uv sync --locked


# COPY from host machine to a directory inside the container
# COPY pipeline.py . 
COPY ingest_data.py .

# ENTRYPOINT ["uv", "run", "python", "pipeline.py"] -- to not write uv run here, use the ENV PATH
ENTRYPOINT ["python", "pipeline.py"]