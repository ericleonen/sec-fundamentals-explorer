# Dockerfile
FROM bitnami/spark:3.4.1

ENV PYSPARK_PYTHON=python3
ENV SPARK_MODE=standalone

# Use root to install tools
USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Create a non-root dev user
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID
RUN groupadd --gid $USER_GID $USERNAME && \
    useradd --uid $USER_UID --gid $USER_GID -m $USERNAME && \
    chown -R $USERNAME /home/$USERNAME

# Install Python dependencies
COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Create folders and set permissions
RUN mkdir -p /workspace/scripts /workspace/data /workspace/notebooks && \
    chown -R $USERNAME /workspace

# Set working directory and switch to user
WORKDIR /workspace
USER $USERNAME