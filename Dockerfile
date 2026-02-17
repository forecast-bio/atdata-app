# syntax=docker/dockerfile:1

# --- Builder stage: install dependencies with uv ---
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# Install dependencies first (cache-friendly layer ordering)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Install the project itself
COPY src/ src/
COPY README.md ./
RUN uv sync --frozen --no-dev

# --- Runtime stage: minimal image ---
FROM python:3.12-slim

WORKDIR /app

# Copy the virtual environment from the builder
COPY --from=builder /app/.venv /app/.venv

# Put the venv on PATH
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Non-root user
RUN groupadd --system app && useradd --system --gid app app
USER app

EXPOSE 8000

# Railway sets PORT; fall back to 8000 for local use
CMD uvicorn atdata_app.main:app --host 0.0.0.0 --port ${PORT:-8000}
