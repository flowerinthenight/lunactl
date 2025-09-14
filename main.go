package main

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func main() {
	const addr = "127.0.0.1:9090"
	log.Printf("Go Arrow client connecting to %s", addr)

	// 1. Connect to the TCP server
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}

	defer conn.Close()
	log.Println("Successfully connected.")

	_, err = conn.Write([]byte("+20\r\nHello from Go client-ex\r\n"))
	if err != nil {
		log.Fatalf("Failed to send greeting: %v", err)
	}

	// 2. Create an Arrow IPC stream reader from the connection
	// The reader will read from the 'conn' (which implements io.Reader).
	// It will first read and decode the schema.
	r, err := ipc.NewReader(conn)
	if err != nil {
		log.Fatalf("Failed to create Arrow reader: %v", err)
	}
	defer r.Release()

	log.Println("Received schema:", r.Schema())

	// 3. Loop to read all RecordBatches from the stream
	var recordCount int
	for r.Next() {
		func() {
			// Get the current record (RecordBatch)
			rec := r.RecordBatch()

			// It's important to release the record's memory when you're done.
			// A defer here would wait until the function exits, so we explicitly
			// call Release at the end of the loop.
			defer rec.Release()

			recordCount++
			log.Printf("--- Reading Record Batch #%d ---", recordCount)
			log.Printf("Rows: %d, Columns: %d", rec.NumRows(), rec.NumCols())

			// Print the contents of the record for verification
			// In a real application, you would process the data here.
			fmt.Println(rec)
		}()
	}

	// 4. Check for any errors that occurred during reading
	if err := r.Err(); err != nil && err != io.EOF {
		log.Fatalf("Error reading records: %v", err)
	}

	log.Printf("Finished reading %d record batches from stream.", recordCount)
}
