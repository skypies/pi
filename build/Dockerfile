# cd ~/repo/skypies/pi
# make -f build/Makefile docker-image

# see ./Makefile ... life is too short to use docker to build anything
# complex, so the makefile does all that.

FROM scratch

COPY ./build/consolidator /
COPY ./build/auth.json /
COPY ./build/ca-certificates.crt /etc/ssl/certs/

ENV GOOGLE_APPLICATION_CREDENTIALS /auth.json

CMD ["/consolidator", "-ae=false"]

EXPOSE 8080

# https://gist.github.com/PurpleBooth/ec81bad0a7b56ac767e0da09840f835a#file-dockerfile

#FROM golang:1.9

#WORKDIR /go/src/github.com/purplebooth/example
#COPY . .
#RUN go build -ldflags "-linkmode external -extldflags -static" -a main.go

#FROM scratch
#COPY --from=0 /go/src/github.com/purplebooth/example/main /main
#CMD ["/main"]
