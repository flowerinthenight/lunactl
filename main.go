package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"

	"github.com/apache/arrow-go/v18/arrow/ipc"
)

var (
	flagPrefix = flag.String("prefix", "$", "Request prefix")
	flagType   = flag.String("type", "q:", "Command type")
)

func main() {
	flag.Parse()
	const addr = "127.0.0.1:7688"

	args := os.Args
	_ = args
	if len(args) < 2 {
		slog.Info("missing argument")
		return
	}

	slog.Info("connecting:", "addr", addr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		slog.Error("Dial failed:", "err", err)
		return
	}

	defer conn.Close()
	slog.Info("connected")

	payload := fmt.Sprintf("%s%d\r\n%s%s\r\n", *flagPrefix, len(args[1])+len(*flagType), *flagType, args[1])
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
