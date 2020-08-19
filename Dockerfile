FROM cnvrg/cnvrg_spark

# requiremnets install
COPY . /workspace/package/

# install requirements
RUN pip3 --disable-pip-version-check --no-cache-dir install -r /workspace/package/requirements.txt 

WORKDIR /workspace/package/

ENTRYPOINT ["pytest", "tests/"]
