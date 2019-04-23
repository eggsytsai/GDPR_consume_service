FROM python:3.6

MAINTAINER Eggsy(eggsy.tsai@grindr.com)

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY

RUN apt-get update && apt-get install libsasl2-dev
RUN apt-get install awscli -y
RUN pip install pika
RUN pip install boto
RUN pip install sshtunnel
RUN pip install bit_array
RUN pip install thriftpy==0.3.9
RUN pip install thriftpy2==0.4.0
RUN pip install six
RUN pip install thrift_sasl
RUN pip install sasl
RUN pip install impyla==0.13.8
RUN pip install kazoo
RUN aws s3 cp s3://datalake.preprod.grindr.io/user/eggsy/GDPR_prod.py /app/
ENTRYPOINT ["python", "/app/GDPR_prod.py"]
