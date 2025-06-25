FROM continuumio/miniconda3

WORKDIR /app

COPY environment.yml .

RUN conda env update -n base -f environment.yml

COPY . .

EXPOSE 8050

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8050"]
