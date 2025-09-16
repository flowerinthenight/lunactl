package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"

	"github.com/apache/arrow-go/v18/arrow/ipc"
)

var (
	flagHostPort = flag.String("hp", "127.0.0.1:7688", "Luna's host:port")
	flagPrefix   = flag.String("prefix", "$", "Request prefix")
	flagType     = flag.String("type", "q:", "Command type")
	flagPayload  = flag.String("p", "SHOW tables;", "Main payload")
)

func main() {
	flag.Parse()
	if *flagPayload == "" {
		slog.Info("missing payload")
		return
	}

	slog.Info("connecting:", "addr", *flagHostPort)
	conn, err := net.Dial("tcp", *flagHostPort)
	if err != nil {
		slog.Error("Dial failed:", "err", err)
		return
	}

	defer conn.Close()
	slog.Info("connected")

	payload := fmt.Sprintf("%s%d\r\n%s%s\r\n", *flagPrefix, len(*flagPayload)+len(*flagType), *flagType, *flagPayload)
	slog.Info("send:", "payload", payload)

	_, err = conn.Write([]byte(payload))
	if err != nil {
		slog.Error("Write failed:", "err", err)
		return
	}

	r, err := ipc.NewReader(conn)
	if err != nil {
		slog.Error("NewReader failed:", "err", err)
		return
	}

	defer r.Release()
	slog.Info("schema received:")
	fmt.Println(r.Schema())

	var cnt int
	for r.Next() {
		func() {
			rec := r.RecordBatch()
			defer rec.Release()

			slog.Info(fmt.Sprintf("Reading RecordBatch[%d]", cnt))
			slog.Info("table:", "rows", rec.NumRows(), "cols", rec.NumCols())
			fmt.Println(rec)
			cnt++
		}()
	}

	if err := r.Err(); err != nil && err != io.EOF {
		slog.Error("Read failed:", "err", err)
		return
	}

	slog.Info("finished:", "recordCount", cnt)
}
