FROM python:latest

# install arango client
RUN curl https://download.arangodb.com/arangodb38/Community/Linux/arangodb3-client_3.8.4-1_amd64.deb -o /tmp/arangodb3-client_3.8.4-1_amd64.deb
RUN dpkg -i /tmp/arangodb3-client_3.8.4-1_amd64.deb



RUN curl http://ftp.fr.debian.org/debian/pool/main/p/postgresql-common/postgresql-client-common_200+deb10u4_all.deb -o /tmp/pg_common.deb
RUN dpkg -i /tmp/pg_common.deb
RUN curl http://ftp.fr.debian.org/debian/pool/main/p/postgresql-11/postgresql-client-11_11.14-0+deb10u1_amd64.deb -o /tmp/pg_client.deb
RUN dpkg -i /tmp/pg_client.deb
# RUN curl http://ftp.fr.debian.org/debian/pool/main/p/postgresql-14/postgresql-client-14_14.2-1+b2_amd64.deb -o /tmp/pg_client.deb
# RUN dpkg -i /tmp/pg_client.deb

RUN pip install git+https://github.com/1lca/stix2arango