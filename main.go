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
	flagAddr    = flag.String("addr", "127.0.0.1:7688", "Luna's host:port")
	flagPrefix  = flag.String("prefix", "$", "Request prefix")
	flagExec    = flag.Bool("x", false, "Execute command type")
	flagQuery   = flag.Bool("q", false, "Query command type (higher priority than -x)")
	flagPayload = flag.String("p", "SHOW tables;", "Main payload")
	flagPass    = flag.String("pass", "", "Password (when AUTH is required)")
)

func main() {
	flag.Parse()
	if *flagPayload == "" {
		slog.Info("missing payload")
		return
	}

	slog.Info("connecting:", "addr", *flagAddr)
	conn, err := net.Dial("tcp", *flagAddr)
	if err != nil {
		slog.Error("Dial failed:", "err", err)
		return
	}

	defer conn.Close()
	slog.Info("connected")

	payloads := []string{}
	if *flagPass != "" {
		payloads = append(payloads, fmt.Sprintf("AUTH %s\r\n", *flagPass))
	}

	cmdType := "q:" // default
	if *flagExec {
		cmdType = "x:"
	}

	if *flagQuery {
		cmdType = "q:"
	}

	payloads = append(payloads, fmt.Sprintf("%s%d\r\n%s%s\r\n",
		*flagPrefix,
		len(*flagPayload)+len(cmdType),
		cmdType,
		*flagPayload),
	)

	for _, payload := range payloads {
		_, err = conn.Write([]byte(payload))
		if err != nil {
			slog.Error("Write failed:", "err", err)
			return
		}

		slog.Info("send:", "payload", payload)

		rdr, err := ipc.NewReader(conn)
		if err != nil {
			slog.Error("NewReader failed:", "err", err)
			return
		}

		slog.Info("schema received:")
		fmt.Println(rdr.Schema())

		var cnt int
		for rdr.Next() {
			rec := rdr.RecordBatch()
			slog.Info(fmt.Sprintf("Reading RecordBatch[%d]", cnt))
			slog.Info("table:", "rows", rec.NumRows(), "cols", rec.NumCols())
			fmt.Println(rec)
			rec.Release()
			cnt++
		}

		if err := rdr.Err(); err != nil && err != io.EOF {
			slog.Error("Read failed:", "err", err)
			return
		}

		slog.Info("finished:", "records", cnt)
		rdr.Release()
	}
}
