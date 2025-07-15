FROM public.ecr.aws/lambda/python@sha256:a5196d38ac1edfec38e17da1002c80b62533773770edf391b6c70a239006ba2a

# Install build tools needed for pymupdf
RUN yum install -y gcc gcc-c++ make python3-devel

WORKDIR /var/task

RUN pip install --upgrade pip \
 && pip install --prefer-binary \
    fastapi \
    sentence-transformers \
    boto3 \
    opensearch-py \
    requests \
    mangum \
    python-magic \
    PyMuPDF \
    openai

COPY main.py .
COPY utils.py .

CMD ["main.handler"]