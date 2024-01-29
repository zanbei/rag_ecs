FROM public.ecr.aws/docker/library/python:3.10.12-slim as build
WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt
COPY main.py ./

ENTRYPOINT ["/usr/local/bin/python3", "-u", "main.py"]
