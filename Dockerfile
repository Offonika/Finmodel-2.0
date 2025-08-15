FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV FINMODEL_SCRIPT=finmodel.scripts.saleswb_import_flat

CMD ["sh", "-c", "python -m ${FINMODEL_SCRIPT}"]
