# Use the Python 3.10 base image
FROM python:3.10

# Install the kafka-python library
RUN pip install kafka-python

# Copy the InfinityImage.py file into the current directory
ADD InfinityImage.py .

# Set the command to run when the image is started
CMD ["python", "/InfinityImage.py"]