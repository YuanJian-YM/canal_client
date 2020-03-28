from harbor.shannonai.com/public/inf-python:v1.1.9

LABEL maintainer="jian_yuan@shannonai.com" \
      namespaces="public"

WORKDIR /home/work

COPY . .

RUN pip install --no-cache-dir --default-timeout=120 -r /home/work/requirements.txt

ENV PYTHONPATH /home/work

ENTRYPOINT ["python", "/home/work/main.py"]


