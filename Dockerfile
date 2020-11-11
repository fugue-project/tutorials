FROM fugueproject/devenv:0.1.7

RUN pip install fugue[all]==0.4.6

ARG NB_USER=root
#ARG NB_UID=1000
ENV USER ${NB_USER}
#ENV NB_UID ${NB_UID}
ENV HOME /home/${NB_USER}

WORKDIR ${HOME}

USER root
RUN rm -rf ${HOME}
COPY README.ipynb ${HOME}/
COPY tutorials ${HOME}/tutorials
COPY data ${HOME}/data
COPY .jupyter ${HOME}/.jupyter
COPY images ${HOME}/images
#RUN chown -R ${NB_UID} ${HOME}

USER ${NB_USER}
# RUN pip install -r requirements.txt