# Python version used
FROM python:3.8

#Through the ADD command we add classes in the docker image
ADD main.py .

#Through the RUN command we install the libraries necessary to start the code
RUN  pip install kafka-python

#This command specifies the default command to run when the container starts. In this case, it runs the main.py Python script.
CMD [ "python", "./main.py" ]