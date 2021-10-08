FROM fugueproject/notebook:0.2.4

ARG NB_USER=vscode
ARG NB_UID=1000
ENV USER ${NB_USER}
ENV NB_UID ${NB_UID}
ENV HOME /home/${NB_USER}

WORKDIR ${HOME}

USER root
# RUN rm -rf ${HOME}
# COPY README.ipynb ${HOME}/
COPY README.md ${HOME}/
COPY tutorials ${HOME}/tutorials
COPY data ${HOME}/data
COPY .jupyter ${HOME}/.jupyter
COPY images ${HOME}/images

RUN mkdir -p /home/${NB_USER}/.jupyter
RUN cp -R /root/.jupyter/* /home/${NB_USER}/.jupyter

USER root
# RUN chown -R ${NB_UID} ${HOME}
USER ${NB_USER}
