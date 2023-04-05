FROM fugueproject/notebook:0.5.4

ENV NB_USER vscode
ENV NB_UID 1000
ENV USER ${NB_USER}
ENV NB_UID ${NB_UID}
ENV HOME /home/${NB_USER}

WORKDIR ${HOME}

USER root

COPY README.md ${HOME}/
COPY tutorials ${HOME}/tutorials
COPY data ${HOME}/data
COPY .jupyter ${HOME}/.jupyter
COPY images ${HOME}/images
RUN rm ${SPARK_CONF_DIR}/spark-defaults.conf
COPY spark-defaults.conf ${SPARK_CONF_DIR}/

USER root
RUN chown -R ${NB_UID} ${HOME}
USER ${NB_USER}

WORKDIR ${HOME}
