FROM python:3.7.7

LABEL maintainer='Luc Somers'

WORKDIR /etl-projects

ARG AIRFLOW_VERSION=1.10.3
ENV PYTHONIOENCODING=utf-8 \
    AIRFLOW_HOME=/etl-projects/airflow_python \
    PYTHONPATH="${PYTHONPATH}:/${AIRFLOW_HOME}/config" \
    SLUGIFY_USES_TEXT_UNIDECODE=yes \
    AIRFLOW_GPL_UNIDECODE=yes

RUN apt-get update && \
    apt-get install -y \
    python-pip \
    python-dev \
    python-setuptools \
    libpq-dev \
    libghc-persistent-postgresql-dev \
    build-essential \
    autoconf \
    libtool \
    libssl-dev \
    libffi-dev \
    vim \
    git \
    jq \
    locales

COPY requirements.txt .

# This step will be improve with a new step that get your github authentication from your machine.
# Until there, let's use this primitive way :D
RUN python3 -m pip install --upgrade pip && \
    # git config --global url.https://<GITHUB_TOKEN>:@github.com/.insteadOf https://github.com/ && \
    pip install -r requirements.txt

RUN pip uninstall -y enum34

COPY . .

RUN chmod +x start.sh

ENTRYPOINT /etl-projects/start.sh
