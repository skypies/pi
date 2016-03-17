all:
	GOOS=linux GOARCH=arm go build -o skypi.arm skypi/skypi.go
	GOOS=linux GOARCH=arm go build -o pinger.arm pinger/pinger.go
