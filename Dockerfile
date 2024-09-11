FROM apache/airflow:2.9.3

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy custom modules and plugins
COPY data_transformation /opt/airflow/data_transformation
COPY plugins /opt/airflow/plugins

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Set permissions if necessary
USER airflow
