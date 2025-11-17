FROM ubuntu:latest
LABEL authors="alexk"

ENTRYPOINT ["top", "-b"]