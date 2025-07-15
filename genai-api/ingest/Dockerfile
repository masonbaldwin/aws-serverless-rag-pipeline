FROM public.ecr.aws/lambda/python:3.11

# Install build dependencies
RUN yum install -y gcc gcc-c++ python3-devel zip \
 && python3 -m ensurepip \
 && pip3 install --upgrade pip \
 && mkdir -p /tmp/build

WORKDIR /tmp/build

# Install Python dependencies, including xlsx support
RUN pip3 install \
    "unstructured[xlsx]==0.10.30" \
    numpy==1.26.4 \
    opensearch-py \
    requests \
    requests-aws4auth \
    typing_extensions \
    -t .

# Copy your handler.py into the build directory
COPY handler.py .

# Zip everything into /tmp/ingest.zip
RUN zip -r9 /tmp/ingest.zip .