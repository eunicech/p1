package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	client, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		return nil, errors.New("Failed to connect to server")
	}
	joinMsg := bitcoin.NewJoin()
	msg, err := json.Marshal(joinMsg)
	if err != nil {
		return nil, errors.New("error marshalling join message")
	}
	client.Write(msg)
	return client, nil
}

// getNonce computes the hash of nonce concatenated with string
// for each nonce in range [lower, upper] (inclusive)
// it then returns the minimum hash value and the nonce corresponding
// to that minimum hash
func getNonce(data string, lower uint64, upper uint64) (uint64, uint64) {
	var maxNonce, hash uint64 = 0, ^uint64(0)
	for i := lower; i <= upper; i++ {
		temp := bitcoin.Hash(data, i)
		if temp < hash {
			hash = temp
			maxNonce = i
		}
	}
	return hash, maxNonce
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()
	for {
		// read requests from the server and stop reading if there is an error
		msg, err := miner.Read()
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			break
		}
		var request bitcoin.Message
		json.Unmarshal(msg, &request)

		// compute the minimum hash and return the corresponding nonce
		hash, nonce := getNonce(request.Data, request.Lower, request.Upper)

		// send result back to server
		result := bitcoin.NewResult(hash, nonce)
		resMsg, _ := json.Marshal(result)
		miner.Write(resMsg)
	}
}
