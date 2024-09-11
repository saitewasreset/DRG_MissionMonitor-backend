FROM python:3
WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY api ./api

CMD ["gunicorn", "api:create_app()", "-b", "0.0.0.0", "-t", "3600"]