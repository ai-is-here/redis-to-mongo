# Use an official Python runtime as a parent image
FROM python:3.11.5

# Set the working directory in the container to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
RUN mkdir /app/redis_to_mongo
# Install any needed packages specified in requirements.txt
WORKDIR /app/redis_to_mongo
COPY setup.py /app/redis_to_mongo/
COPY requirements.txt /app/redis_to_mongo/
RUN pip install -r requirements.txt
COPY ./redis_to_mongo /app/redis_to_mongo/redis_to_mongo/
RUN pip install --no-cache-dir .

# Run your_script.py when the container launches
CMD ["sh", "-c", "if [ \"$MODE\" != 'test' ]; then python ./redis_to_mongo/run.py $MODE; fi"]
#CMD ls ./redis_to_mongo