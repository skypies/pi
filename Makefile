all:
	GOOS=linux GOARCH=arm go build -o client.arm client/client.go
	GOOS=linux GOARCH=arm go build -o client.arm skypi/skypi.go
