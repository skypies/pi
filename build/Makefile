# cd ~/repo/skypies/pi
# make -f build/Makefile docker-image
# docker run -p8080:8080 consolidator

# auth.json is for the GOOGLE_APPLICATION_CREDENTIALS
AUTHJSON=~/auth.json

GOFLAGS=-ldflags "-linkmode external -extldflags -static" -a

consolidator:
	go build ${GOFLAGS} -o build/consolidator cmd/consolidator/consolidator.go

# Copy all the crap we need into ./build, so that the dockerfile can see it.
docker-image: consolidator
	cp /etc/ssl/certs/ca-certificates.crt ./build
	cp ${AUTHJSON} ./build
	docker build -t consolidator -f ./build/Dockerfile .

# Next ... publish to gcr.io
# https://cloud.google.com/container-registry/docs/pushing-and-pulling

skypi:
	GOOS=linux GOARCH=arm go build -o skypi.arm cmd/skypi/skypi.go

DESTNAME=`TZ="America/Los_Angeles" date +"consolidator-%Y%m%d-%H%M"`
publish: consolidator
	scp build/consolidator adam@35.239.5.96:~/${DESTNAME}
