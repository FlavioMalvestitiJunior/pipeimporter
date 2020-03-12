FROM debian
LABEL maintainer="Ailin Albertoni <ailinh@chaordicsystems.com>"

RUN apt-get update
RUN apt-get install -y python3 
RUN apt-get install -y python3-pip 
RUN apt-get install -y locales locales-all

ENV LANG pt_BR.UTF-8
ENV LANGUAGE pt_BR.UTF-8

WORKDIR /opt

COPY . /opt

RUN pip3 install openpyxl

ENTRYPOINT ["python3"]
CMD ["app.py"]