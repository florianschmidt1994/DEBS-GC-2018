FROM python:3.6-slim
RUN pip install 'scikit-learn==0.18.1' pika numpy scipy pandas bitstring
COPY . /usr/src/app/
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "/usr/src/app/main.py"]