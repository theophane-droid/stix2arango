FROM python:latest

COPY requirements.txt /requirements.txt

RUN pip3 install -r /requirements.txt
RUN pip install git+https://github.com/theophane-droid/stix2arango.git --target /additional_packages
RUN pip install git+https://github.com/theophane-droid/stix2arango.git

CMD python3 /app/test/test01.py