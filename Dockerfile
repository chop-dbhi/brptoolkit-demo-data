FROM golang:1.4


RUN mkdir -p /go/src/app

WORKDIR /go/src/app

# Oracle Drivers
RUN apt-get update -qq
RUN apt-get install libaio1 pkg-config -y

ADD . /go/src/app

RUN cp /go/src/app/pkg/oracle/oci8.pc /usr/lib/pkgconfig/
RUN ln -s /go/src/app/pkg/oracle/instantclient_11_2/libclntsh.so.11.1 /go/src/app/pkg/oracle/instantclient_11_2/libclntsh.so
RUN ln -s /go/src/app/pkg/oracle/instantclient_11_2/libocci.so.11.1 /go/src/app/pkg/oracle/instantclient_11_2/libocci.so

ENV LD_LIBRARY_PATH /go/src/app/pkg/oracle/instantclient_11_2
ENV ORACLE_HOME /go/src/app/pkg/oracle/instantclient_11_2

RUN go-wrapper download
RUN go-wrapper install

ENTRYPOINT ["/go/bin/app"]
