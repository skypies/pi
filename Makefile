all:
	GOOS=linux GOARCH=arm go build -o skypi.arm skypi/skypi.go
