# # DO NOT ask me wtf is up with these comments i need it ok
# # i wil forget what is what
# # Yes i am THE genuine competition to a goldfish memory

FROM python:3.10-slim-bullseye

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements_docker.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements_docker.txt

COPY app.py .
COPY network_security ./network_security
COPY templates ./templates
COPY Final_Model ./Final_Model
COPY data_schema ./data_schema
COPY valid_data ./valid_data

EXPOSE 8000

# Changed from /health to /model/info — /health is not defined in app.py.
# /model/info is a lightweight JSON endpoint that proves the app and model loaded.
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s \
  CMD curl -f http://localhost:8000/model/info || exit 1

# --workers 1 is intentional: training uses background threads and shared state.
# Multiple workers would give each worker its own training state, breaking /train/status.
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]



# # -----------------------------------------------------------------------------
# # Use an official lightweight Python image as the base image.
# #
# # python:3.10-slim-buster:
# # - Python 3.10 preinstalled
# # - Debian Buster based
# # - "slim" version reduces unnecessary packages to keep image lightweight
# #
# # Benefits:
# # - Smaller image size
# # - Faster deployment
# # - Reduced attack surface
# # -----------------------------------------------------------------------------
# FROM python:3.10-slim-bullseye


# # -----------------------------------------------------------------------------
# # Set environment variables for better Python behavior inside Docker.
# #
# # PYTHONDONTWRITEBYTECODE=1
# # -> Prevents Python from creating .pyc cache files
# # -> Keeps container filesystem cleaner
# #
# # PYTHONUNBUFFERED=1
# # -> Ensures logs are printed directly to terminal without buffering
# # -> Very useful for Docker logs and debugging
# # -----------------------------------------------------------------------------
# ENV PYTHONDONTWRITEBYTECODE=1 \
#     PYTHONUNBUFFERED=1


# # -----------------------------------------------------------------------------
# # Set the working directory inside the container.
# #
# # All future commands will execute relative to /app
# # Example:
# # - COPY . .  -> copies files into /app
# # - RUN pip install -> executed from /app
# # -----------------------------------------------------------------------------
# WORKDIR /app


# # -----------------------------------------------------------------------------
# # Install required system dependencies.
# #
# # apt-get update
# # -> Refresh package index from Debian repositories
# #
# # apt-get install
# # -> Installs required OS-level packages
# #
# # build-essential:
# # -> Required for compiling some Python packages
# #
# # gcc:
# # -> GNU C compiler needed for native extensions
# #
# # curl:
# # -> Useful for debugging, health checks, APIs, etc.
# #
# # rm -rf /var/lib/apt/lists/*
# # -> Cleans cached package lists to reduce image size
# #
# # NOTE:
# # Add additional Linux packages here if your ML libraries require them.
# # -----------------------------------------------------------------------------
# RUN apt-get update && apt-get install -y \
#     # build-essential \
#     # gcc \
#     curl \
#     && rm -rf /var/lib/apt/lists/*


# # -----------------------------------------------------------------------------
# # Copy only requirements.txt first.
# #
# # WHY?
# # Docker caches layers.
# #
# # If requirements.txt does NOT change:
# # -> Docker reuses cached dependency installation layer
# # -> Future builds become MUCH faster
# #
# # This is a Docker optimization best practice.
# # -----------------------------------------------------------------------------
# # COPY requirements.txt .
# COPY requirements_docker.txt .
# # -----------------------------------------------------------------------------
# # Upgrade pip to latest stable version.
# #
# # Then install all Python dependencies from requirements.txt
# #
# # --no-cache-dir:
# # -> Prevents pip from storing package cache
# # -> Reduces final image size
# #
# # Example dependencies:
# # - fastapi
# # - uvicorn
# # - pandas
# # - scikit-learn
# # - tensorflow
# # etc.
# # -----------------------------------------------------------------------------
# RUN pip install --upgrade pip && \
#     pip install --no-cache-dir -r requirements_docker.txt


# # -----------------------------------------------------------------------------
# # Copy the entire project into the container.
# #
# # Everything from current host directory gets copied into /app
# #
# # Includes:
# # - Python source files
# # - ML models
# # - configs
# # - utility scripts
# # - templates/static files
# # -----------------------------------------------------------------------------
# # COPY . .
# COPY app.py .
# COPY network_security ./network_security
# COPY templates ./templates
# COPY Final_Model ./Final_Model
# COPY data_schema ./data_schema

# # -----------------------------------------------------------------------------
# # Create required directories inside container.
# #
# # mkdir -p:
# # -> Creates directories recursively if they don't exist
# # -> Does not fail if directories already exist
# #
# # Directory purposes:
# #
# # Final_Model
# # -> Stores trained/final ML models
# #
# # Artifacts
# # -> Stores preprocessing artifacts, encoders, transformers, etc.
# #
# # Predicted_Output
# # -> Stores prediction results generated by the app
# #
# # logs
# # -> Stores application/runtime logs
# # -----------------------------------------------------------------------------
# # RUN mkdir -p \
# #     Final_Model \
# #     Artifacts \
# #     Predicted_Output \
# #     logs
# # RUN mkdir -p \
# #     Final_Model
#     # Artifacts \
#     # Predicted_Output \
#     # logs


# # -----------------------------------------------------------------------------
# # Expose application port.
# #
# # This tells Docker the container listens on port 8000.
# #
# # IMPORTANT:
# # EXPOSE does NOT publish the port automatically.
# #
# # To access externally:
# # docker run -p 8000:8000 image_name
# # -----------------------------------------------------------------------------
# EXPOSE 8000


# # -----------------------------------------------------------------------------
# # Default command executed when container starts.
# #
# # uvicorn:
# # -> ASGI server for FastAPI applications
# #
# # app:app
# # -> app.py file
# # -> FastAPI instance named "app"
# #
# # --host 0.0.0.0
# # -> Makes server accessible outside container
# #
# # --port 8000
# # -> Application runs on port 8000
# #
# # --workers 1
# # -> Number of worker processes
# # -> Increase in production if needed
# #
# # Example:
# # uvicorn app:app --host 0.0.0.0 --port 8000
# # -----------------------------------------------------------------------------
# CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]