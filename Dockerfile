FROM repo.dev002.local/dockerio/python:3.8-slim-buster
LABEL maintainer="Talgarbaev Gaziz <talgarbaevg@sibur.ru>"

RUN mkdir -p /root/.pip/

ARG PYPI_INDEX_URL
ARG REPO_HOST
ARG PYPI_EXTRA_INDEX_URL

# USE --build-arg PYPI_INDEX_URL=... --build-arg REPO_HOST=...
RUN rm -rf /root/.pip/pip.conf && \
echo "[global] \n\
index-url = ${PYPI_INDEX_URL} \n\
trusted-host = ${REPO_HOST} \n\
extra-index-url = ${PYPI_EXTRA_INDEX_URL}" \
>> /root/.pip/pip.conf

# USE --build-arg PYPI_INDEX_URL=...
RUN rm -rf /root/.pydistutils.cfg && \
echo "[easy_install] \n\
index-url = ${PYPI_INDEX_URL}" \
>> /root/.pydistutils.cfg

RUN mkdir -p /opt/ukd_orc_service/src

COPY requirements.txt /opt/ukd_orc_service/requirements.txt
RUN pip3 install -r /opt/ukd_orc_service/requirements.txt

COPY etl_orchestration_package /opt/ukd_orc_service/src

EXPOSE 9085

USER root
WORKDIR /opt/ukd_orc_service

COPY scripts/entrypoint.sh /usr/local/entrypoint.sh
RUN ["chmod", "+x", "/usr/local/entrypoint.sh"]
ENTRYPOINT ["/usr/local/entrypoint.sh"]
