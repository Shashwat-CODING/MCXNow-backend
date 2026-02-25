FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port 7860 (required for Hugging Face Spaces)
EXPOSE 7860

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV FLASK_ENV=production

# Run the application
CMD ["python", "app.py"]
