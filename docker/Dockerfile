# Dockerfile for ibm cloud function dependencies
FROM ibmfunctions/action-python-v3.9

COPY requirements.txt .

RUN pip install --upgrade pip

RUN pip install -r requirements.txt --upgrade