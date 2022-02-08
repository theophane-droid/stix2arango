FROM python:latest

# install arango client
RUN curl https://download.arangodb.com/arangodb38/Community/Linux/arangodb3-client_3.8.4-1_amd64.deb -o /tmp/arangodb3-client_3.8.4-1_amd64.deb
RUN dpkg -i /tmp/arangodb3-client_3.8.4-1_amd64.deb

RUN pip install git+https://github.com/theophane-droid/stix2arango.git

RUN pip install flask_login