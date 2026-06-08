


FROM python:3.9-slim
WORKDIR /app
COPY . .
COPY bigdatafinance-491008-3a9a6f5330b0.json /app/credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json
RUN pip install google-cloud-pubsub flask scrapy billiard  mysql-connector-python pytz pandas Twisted requests findspark seaborn numpy matplotlib pyspark scikit_learn pyspark yfinance 

# CMD ["python", "producer.py"]
ENV PORT=8080
CMD ["python", "-u", "producer.py"]