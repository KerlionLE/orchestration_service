FROM repo.dev002.local/dockerio/python:3.8-slim-buster
LABEL maintainer="Talgarbaev Gaziz <talgarbaevg@sibur.ru>"

RUN mkdir -p /root/.pip/

ARG PYPI_INDEX_URL
ARG REPO_HOST
ARG PYPI_EXTRA_INDEX_URL
ENV WORK_DIR='/opt/ukd_orc_service'

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

RUN mkdir -p ${WORK_DIR}/src

COPY etl_orchestration_package/requirements.txt ${WORK_DIR}/requirements.txt
RUN pip3 install -r ${WORK_DIR}/requirements.txt

COPY etl_orchestration_package ${WORK_DIR}/src

EXPOSE 9085

USER root
WORKDIR ${WORK_DIR}

COPY scripts/entrypoint.sh /usr/local/entrypoint.sh
RUN cd ${WORK_DIR}
RUN ["chmod", "+x", "/usr/local/entrypoint.sh"]
ENTRYPOINT ["/usr/local/entrypoint.sh"]
