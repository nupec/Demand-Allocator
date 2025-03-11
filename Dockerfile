FROM continuumio/miniconda3

WORKDIR /app

COPY environment.yml .

RUN conda update -n base -c defaults conda && \
    conda config --set remote_connect_timeout_secs 60 && \
    conda config --set remote_max_retries 3 && \
    conda env create -f environment.yml

ENV PATH=/opt/conda/envs/demand-allocator/bin:$PATH

COPY . .

EXPOSE 8000

CMD ["uvicorn", "run:app", "--host", "0.0.0.0", "--port", "8000"]
