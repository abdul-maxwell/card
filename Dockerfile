FROM python:3.12-slim  # Explicitly uses 3.12

WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "bot.py"]
