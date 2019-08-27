FROM python:3.6

MAINTAINER Eggsy(eggsy.tsai@grindr.com)

RUN apt-get update && apt-get install libsasl2-dev
RUN pip install pika \
 boto \
 sshtunnel \
 bit_array \
 thriftpy==0.3.9 \
 thriftpy2==0.4.0 \
 six \
 thrift_sasl \
 sasl \
 impyla==0.13.8 \
 kazoo

COPY GDPR_prod.py /app/

ENTRYPOINT ["python", "/app/GDPR_prod.py"]