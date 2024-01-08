FROM python:3.11.4

# Set the working directory inside the container
WORKDIR /task_data

# Copy files from the host machine to the container's working directory
COPY ./data .

# Install dependencies
RUN pip install dask==2023.12.1 sqlalchemy==1.4.41

RUN apt update
RUN apt install vim -y

