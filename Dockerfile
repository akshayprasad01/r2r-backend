# Set environment variables for Spark and Hadoop versions
FROM python:3.12

# # Install Java (OpenJDK)
# RUN apt-get update && \
#     apt-get install -y curl && \
#     apt-get install -y default-jdk && \
#     rm -rf /var/lib/apt/lists/*

# # Install Scala 2.13
# RUN wget -O /tmp/scala-2.13.13.tgz https://downloads.lightbend.com/scala/2.13.13/scala-2.13.13.tgz && \
#     tar xf /tmp/scala-2.13.13.tgz -C /opt && \
#     rm /tmp/scala-2.13.13.tgz

# # Install Spark
# RUN wget -O /tmp/spark-3.3.1-bin-hadoop3-scala2.13.tgz https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3-scala2.13.tgz  && \
#     tar xf /tmp/spark-3.3.1-bin-hadoop3-scala2.13.tgz -C /opt && \
#     rm /tmp/spark-3.3.1-bin-hadoop3-scala2.13.tgz

# # Set environment variables
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# ENV SPARK_HOME=/opt/spark-3.3.1-bin-hadoop3-scala2.13
# ENV PYSPARK_PYTHON=/usr/bin/python3

# # Add Spark binaries to PATH
# ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Set the working directory in the container
WORKDIR /recon

COPY requirements.txt requirements.txt

RUN chmod -R 777 /recon

# Install any needed dependencies specified in requirements.txt
RUN pip install -r requirements.txt

COPY . .

RUN chmod -R 777 /recon 

# Expose the port number on which your FastAPI application will run
EXPOSE 8000

# Command to run the FastAPI application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload", "--reload-exclude", "'bin/*'"]