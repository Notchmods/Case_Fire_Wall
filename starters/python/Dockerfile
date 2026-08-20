# Obsidio starter: Python. Pinned base image for reproducible builds.
FROM python:3.12.7-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py ./
EXPOSE 8080
# One worker on purpose (the naive baseline). This is a knob you will want to
# revisit once you understand the core-count note in the guide.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
