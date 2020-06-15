FROM fugueproject/fuguebase:0.0.7

ARG NB_USER=fugue
ARG NB_UID=1000
ENV USER ${NB_USER}
ENV NB_UID ${NB_UID}
ENV HOME /home/${NB_USER}

WORKDIR ${HOME}

USER root
RUN rm -rf ${HOME}
COPY index.ipynb ${HOME}/
COPY tutorials ${HOME}/tutorials
COPY data ${HOME}/data
COPY .jupyter ${HOME}/.jupyter
COPY images ${HOME}/images
RUN chown -R ${NB_UID} ${HOME}

USER ${NB_USER}
# RUN pip install -r requirements.txt