FROM python:2.7
ENV PYTHONUNBUFFERED 1
RUN mkdir /src
WORKDIR /src
ADD requirements.txt /src/
RUN pip install -r requirements.txt
ADD . /src/
CMD python server.py