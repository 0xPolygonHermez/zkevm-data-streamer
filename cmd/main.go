package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastream"
	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/ethereum/go-ethereum/common"
	"github.com/urfave/cli/v2"
	"google.golang.org/protobuf/proto"
)

const (
	StSequencer = 1 // StSequencer sequencer stream type

	BookmarkBatch   datastream.BookmarkType = 1 // BookmarkBatch bookmark type
	BookmarkL2Block datastream.BookmarkType = 2 // BookmarkL2Block bookmark type

	BatchTypeRegular  uint32 = 1 // BatchTypeRegular Regula Batch type
	BatchTypeForced   uint32 = 2 // BatchTypeForced Forced Batch type
	BatchTypeInjected uint32 = 3 // BatchTypeInjected Injected Batch type
	BatchTypeInvalid  uint32 = 4 // BatchTypeInvalid Invalid Batch type

	streamerSystemID = 137
	streamerVersion  = 1

	noneType        = "none"
	streamServerURL = "127.0.0.1:6900"
	logLevelInfo    = "log level (debug|info|warn|error)"
)

var (
	initSanityEntry    bool   = false
	initSanityBatch    bool   = false
	initSanityBatchEnd bool   = false
	initSanityBlock    bool   = false
	initSanityBlockEnd bool   = false
	initSanityBookmark bool   = false
	sanityEntry        uint64 = 0
	sanityBatch        uint64 = 0
	sanityBlock        uint64 = 0
	sanityBlockEnd     uint64 = 0
	sanityBatchEnd     uint64 = 0

	sanityBookmarkL2Block uint64 = 0
	sanityBookmarkBatch   uint64 = 0

	sanityForkID    uint64 = 0
	dumpBatchNumber uint64 = 0
	dumpBatchData   string
	initDumpBatch   bool   = false
	dumpEntryFirst  uint64 = 0
	dumpEntryLast   uint64 = 0
	dumpBlockFirst  uint64 = 0
	dumpBlockLast   uint64 = 0
	dumpTotalTx     uint64 = 0
)

// main runs a datastream server or client
func main() {
	// Set log level
	log.Init(log.Config{
		Environment: "development",
		Level:       "debug",
		Outputs:     []string{"stdout"},
	})

	app := cli.NewApp()
	app.Usage = "Run a datastream server/client/relay demo cli app"

	app.Commands = []*cli.Command{
		{
			Name:    "server",
			Aliases: []string{},
			Usage:   "Run datastream server",
			Flags: []cli.Flag{
				&cli.Uint64Flag{
					Name:        "port",
					Usage:       "exposed port for clients to connect",
					Value:       6900, //nolint:mnd
					DefaultText: "6900",
				},
				&cli.StringFlag{
					Name:        "file",
					Usage:       "datastream data file name (*.bin)",
					Value:       "datastream.bin",
					DefaultText: "datastream.bin",
				},
				&cli.StringFlag{
					Name:        "log",
					Usage:       logLevelInfo,
					Value:       "info",
					DefaultText: "info",
				},
				&cli.Uint64Flag{
					Name:        "sleep",
					Usage:       "initial sleep and sleep between atomic operations in ms",
					Value:       0,
					DefaultText: "0",
				},
				&cli.Uint64Flag{
					Name:        "opers",
					Usage:       "number of atomic operations (server will terminate after them)",
					Value:       1000000, //nolint:mnd
					DefaultText: "1000000",
				},
				&cli.Uint64Flag{
					Name:        "writetimeout",
					Usage:       "timeout for write operations on client connections in ms (0=no timeout)",
					Value:       3000, //nolint:mnd
					DefaultText: "3000",
				},
				&cli.Uint64Flag{
					Name:        "inactivitytimeout",
					Usage:       "timeout to kill an inactive client connection in seconds (0=no timeout)",
					Value:       120, //nolint:mnd
					DefaultText: "120",
				},
			},
			Action: runServer,
		},
		{
			Name:    "client",
			Aliases: []string{},
			Usage:   "Run datastream client",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:        "server",
					Usage:       "datastream server address to connect (IP:port)",
					Value:       streamServerURL,
					DefaultText: streamServerURL,
				},
				&cli.StringFlag{
					Name:        "from",
					Usage:       "entry number to start the sync/streaming from (latest|0..N)",
					Value:       "latest",
					DefaultText: "latest",
				},
				&cli.StringFlag{
					Name:  "frombookmark",
					Usage: "bookmark to start the sync/streaming from (0..N) (has preference over --from parameter)",
					Value: noneType,
				},
				&cli.BoolFlag{
					Name:  "header",
					Usage: "query file header information",
					Value: false,
				},
				&cli.StringFlag{
					Name:  "entry",
					Usage: "entry number to query data (0..N)",
					Value: noneType,
				},
				&cli.StringFlag{
					Name:  "bookmark",
					Usage: "entry bookmark to query entry data pointed by it (0..N)",
					Value: noneType,
				},
				&cli.IntFlag{
					Name:  "bookmarktype",
					Usage: "bookmark type used for --bookmark and --frombookmark options (0..255)",
					Value: 0,
				},
				&cli.BoolFlag{
					Name:  "sanitycheck",
					Usage: "when receiving streaming check entry, bookmark, and block sequence consistency",
					Value: false,
				},
				&cli.StringFlag{
					Name:  "dumpbatch",
					Usage: "batch number to dump data (0..N)",
					Value: noneType,
				},
				&cli.StringFlag{
					Name:        "log",
					Usage:       logLevelInfo,
					Value:       "info",
					DefaultText: "info",
				},
			},
			Action: runClient,
		},
		{
			Name:    "relay",
			Aliases: []string{},
			Usage:   "Run datastream relay",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:        "server",
					Usage:       "datastream server address to connect (IP:port)",
					Value:       streamServerURL,
					DefaultText: streamServerURL,
				},
				&cli.Uint64Flag{
					Name:        "port",
					Usage:       "exposed port for clients to connect",
					Value:       7900, //nolint:mnd
					DefaultText: "7900",
				},
				&cli.StringFlag{
					Name:        "file",
					Usage:       "relay data file name (*.bin)",
					Value:       "datarelay.bin",
					DefaultText: "datarelay.bin",
				},
				&cli.StringFlag{
					Name:        "log",
					Usage:       logLevelInfo,
					Value:       "info",
					DefaultText: "info",
				},
				&cli.Uint64Flag{
					Name:        "writetimeout",
					Usage:       "timeout for write operations on client connections in ms (0=no timeout)",
					Value:       3000, //nolint:mnd
					DefaultText: "3000",
				},
				&cli.Uint64Flag{
					Name:        "inactivitytimeout",
					Usage:       "timeout to kill an inactive client connection in seconds (0=no timeout)",
					Value:       120, //nolint:mnd
					DefaultText: "120",
				},
			},
			Action: runRelay,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

// runServer runs a local datastream server and tests its features
func runServer(ctx *cli.Context) error {
	// Set log level
	logLevel := ctx.String("log")
	log.Init(log.Config{
		Environment: "development",
		Level:       logLevel,
		Outputs:     []string{"stdout"},
	})

	log.Info(">> App begin")

	// Parameters
	file := ctx.String("file")
	port := ctx.Uint64("port")
	sleep := ctx.Uint64("sleep")
	numOpersLoop := ctx.Uint64("opers")
	writeTimeout := ctx.Uint64("writetimeout")
	inactivityTimeout := ctx.Uint64("inactivitytimeout")

	if file == "" || port <= 0 {
		return errors.New("bad/missing parameters")
	}

	// Create stream server
	s, err := datastreamer.NewServer(uint16(port), streamerVersion, streamerSystemID, StSequencer, file,
		time.Duration(writeTimeout)*time.Millisecond, time.Duration(inactivityTimeout)*time.Second,
		5*time.Second, nil, 0) //nolint:mnd
	if err != nil {
		return err
	}

	// Start stream server
	err = s.Start()
	if err != nil {
		log.Error(">> App error! Start")
		return err
	}

	// Pause for testing purpose
	time.Sleep(time.Duration(sleep) * time.Millisecond)

	end := make(chan uint8)

	go func(chan uint8) {
		var (
			testRollback   bool
			latestRollback uint64
		)

		init := s.GetHeader().TotalEntries / 5 //nolint:mnd

		// Atomic Operations loop
		for n := uint64(0); n < numOpersLoop; n++ {
			// Start atomic operation
			err = s.StartAtomicOp()
			if err != nil {
				log.Error(">> App error! StartAtomicOp")
				return
			}

			// Add stream entries (sample):
			// 1.Bookmark batch
			_, err := s.AddStreamBookmark(fakeBookmark(BookmarkBatch, init+n))
			if err != nil {
				log.Errorf(">> App error! AddStreamBookmark: %v", err)
			}

			// 2.Batch Start
			_, err = s.AddStreamEntry(datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_BATCH_START),
				fakeDataBatchStart(init+n))
			if err != nil {
				log.Errorf(">> App error! AddStreamEntry type %v: %v",
					datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_BATCH_START), err)
				return
			}

			// 3.Bookmark L2 block
			_, err = s.AddStreamBookmark(fakeBookmark(BookmarkL2Block, init+n))
			if err != nil {
				log.Errorf(">> App error! AddStreamBookmark: %v", err)
			}
			// 4.Block Start
			entryBlockStart, err := s.AddStreamEntry(datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_L2_BLOCK),
				fakeDataBlockStart(init+n, init+n))
			if err != nil {
				log.Errorf(">> App error! AddStreamEntry type %v: %v",
					datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_L2_BLOCK), err)
				return
			}
			// 5.Tx
			numTx := 1 // rand.Intn(20) + 1
			for i := 1; i <= numTx; i++ {
				_, err = s.AddStreamEntry(datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_TRANSACTION),
					fakeDataTx())
				if err != nil {
					log.Errorf(">> App error! AddStreamEntry type %v: %v",
						datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_TRANSACTION), err)
					return
				}
			}
			// 5.Block End
			_, err = s.AddStreamEntry(datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_L2_BLOCK_END),
				fakeDataBlockEnd(init+n))
			if err != nil {
				log.Errorf(">> App error! AddStreamEntry type %v: %v",
					datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_L2_BLOCK_END), err)
				return
			}

			// 5.Batch End
			_, err = s.AddStreamEntry(datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_BATCH_END),
				fakeDataBatchEnd(init+n))
			if err != nil {
				log.Errorf(">> App error! AddStreamEntry type %v: %v",
					datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_BATCH_END), err)
				return
			}

			if !testRollback || entryBlockStart%10 != 0 || latestRollback == entryBlockStart {
				// Commit atomic operation
				err = s.CommitAtomicOp()
				if err != nil {
					log.Error(">> App error! CommitAtomicOp")
					return
				}
			} else {
				// Rollback atomic operation
				err = s.RollbackAtomicOp()
				if err != nil {
					log.Error(">> App error! RollbackAtomicOp")
				}
				latestRollback = entryBlockStart
			}

			// Pause for testing purpose
			time.Sleep(time.Duration(sleep) * time.Millisecond)
		}
		end <- 0
	}(end)

	// Wait for loop to end
	<-end

	log.Info(">> App end")

	return nil
}

func fakeBookmark(bookType datastream.BookmarkType, value uint64) []byte {
	bookmark := datastream.BookMark{Type: bookType}
	b, err := proto.Marshal(&bookmark)
	if err != nil {
		log.Error("error marshalling fake bookmark. Ignoring it....")
	}
	return b
}

func fakeDataBlockStart(blockNum, batchNum uint64) []byte {
	l2Block := datastream.L2Block{
		Number:          blockNum,
		BatchNumber:     batchNum,
		Timestamp:       uint64(time.Now().Unix()),
		DeltaTimestamp:  3, //nolint:mnd
		MinTimestamp:    0,
		L1InfotreeIndex: 1,
		BlockGasLimit:   2100000, //nolint:mnd
		L1Blockhash:     common.Hash{}.Bytes(),
		Hash:            common.Hash{}.Bytes(),
		StateRoot:       common.Hash{}.Bytes(),
		GlobalExitRoot:  common.Hash{}.Bytes(),
		Coinbase:        common.Address{}.Bytes(),
		BlockInfoRoot:   common.Hash{}.Bytes(),
	}
	l2B, err := proto.Marshal(&l2Block)
	if err != nil {
		log.Error("error marshalling fake blocka start. Ignoring it....")
	}
	return l2B
}

func fakeDataTx() []byte {
	//nolint:lll
	encode, err := hex.DecodeString("f918b01b8402549e6083112cd58080b9185c60806040526040518060400160405280600a81526020017f42656e6368546f6b656e00000000000000000000000000000000000000000000815250600090816200004a9190620003dc565b506040518060400160405280600381526020017f42544b000000000000000000000000000000000000000000000000000000000081525060019081620000919190620003dc565b506012600260006101000a81548160ff021916908360ff160217905550348015620000bb57600080fd5b506040516200183c3803806200183c8339818101604052810190620000e19190620004f9565b600260009054906101000a900460ff1660ff16600a620001029190620006ae565b816200010f9190620006ff565b600381905550600354600460003373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002081905550506200074a565b600081519050919050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052604160045260246000fd5b7f4e487b7100000000000000000000000000000000000000000000000000000000600052602260045260246000fd5b60006002820490506001821680620001e457607f821691505b602082108103620001fa57620001f96200019c565b5b50919050565b60008190508160005260206000209050919050565b60006020601f8301049050919050565b600082821b905092915050565b600060088302620002647fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff8262000225565b62000270868362000225565b95508019841693508086168417925050509392505050565b6000819050919050565b6000819050919050565b6000620002bd620002b7620002b18462000288565b62000292565b62000288565b9050919050565b6000819050919050565b620002d9836200029c565b620002f1620002e882620002c4565b84845462000232565b825550505050565b600090565b62000308620002f9565b62000315818484620002ce565b505050565b5b818110156200033d5762000331600082620002fe565b6001810190506200031b565b5050565b601f8211156200038c57620003568162000200565b620003618462000215565b8101602085101562000371578190505b62000389620003808562000215565b8301826200031a565b50505b505050565b600082821c905092915050565b6000620003b16000198460080262000391565b1980831691505092915050565b6000620003cc83836200039e565b9150826002028217905092915050565b620003e78262000162565b67ffffffffffffffff8111156200040357620004026200016d565b5b6200040f8254620001cb565b6200041c82828562000341565b600060209050601f8311600181146200045457600084156200043f578287015190505b6200044b8582620003be565b865550620004bb565b601f198416620004648662000200565b60005b828110156200048e5784890151825560018201915060208501945060208101905062000467565b86831015620004ae5784890151620004aa601f8916826200039e565b8355505b6001600288020188555050505b505050505050565b600080fd5b620004d38162000288565b8114620004df57600080fd5b50565b600081519050620004f381620004c8565b92915050565b600060208284031215620005125762000511620004c3565b5b60006200052284828501620004e2565b91505092915050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b60008160011c9050919050565b6000808291508390505b6001851115620005b9578086048111156200059157620005906200052b565b5b6001851615620005a15780820291505b8081029050620005b1856200055a565b945062000571565b94509492505050565b600082620005d45760019050620006a7565b81620005e45760009050620006a7565b8160018114620005fd576002811462000608576200063e565b6001915050620006a7565b60ff8411156200061d576200061c6200052b565b5b8360020a9150848211156200063757620006366200052b565b5b50620006a7565b5060208310610133831016604e8410600b8410161715620006785782820a9050838111156200067257620006716200052b565b5b620006a7565b62000687848484600162000567565b92509050818404811115620006a157620006a06200052b565b5b81810290505b9392505050565b6000620006bb8262000288565b9150620006c88362000288565b9250620006f77fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff8484620005c2565b905092915050565b60006200070c8262000288565b9150620007198362000288565b9250828202620007298162000288565b915082820484148315176200074357620007426200052b565b5b5092915050565b6110e2806200075a6000396000f3fe608060405234801561001057600080fd5b50600436106100935760003560e01c8063313ce56711610066578063313ce5671461013457806370a082311461015257806395d89b4114610182578063a9059cbb146101a0578063dd62ed3e146101d057610093565b806306fdde0314610098578063095ea7b3146100b657806318160ddd146100e657806323b872dd14610104575b600080fd5b6100a0610200565b6040516100ad9190610b67565b60405180910390f35b6100d060048036038101906100cb9190610c22565b61028e565b6040516100dd9190610c7d565b60405180910390f35b6100ee610380565b6040516100fb9190610ca7565b60405180910390f35b61011e60048036038101906101199190610cc2565b61038a565b60405161012b9190610c7d565b60405180910390f35b61013c610759565b6040516101499190610d31565b60405180910390f35b61016c60048036038101906101679190610d4c565b61076c565b6040516101799190610ca7565b60405180910390f35b61018a6107b5565b6040516101979190610b67565b60405180910390f35b6101ba60048036038101906101b59190610c22565b610843565b6040516101c79190610c7d565b60405180910390f35b6101ea60048036038101906101e59190610d79565b610a50565b6040516101f79190610ca7565b60405180910390f35b6000805461020d90610de8565b80601f016020809104026020016040519081016040528092919081815260200182805461023990610de8565b80156102865780601f1061025b57610100808354040283529160200191610286565b820191906000526020600020905b81548152906001019060200180831161026957829003601f168201915b505050505081565b600081600560003373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060008573ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020819055508273ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff167f8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b9258460405161036e9190610ca7565b60405180910390a36001905092915050565b6000600354905090565b60008073ffffffffffffffffffffffffffffffffffffffff168473ffffffffffffffffffffffffffffffffffffffff16036103fa576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004016103f190610e8b565b60405180910390fd5b600073ffffffffffffffffffffffffffffffffffffffff168373ffffffffffffffffffffffffffffffffffffffff1603610469576040517f08c379a000000000000000000000000000000000000000000000000000000000815260040161046090610f1d565b60405180910390fd5b81600460008673ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020016000205410156104eb576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004016104e290610f89565b60405180910390fd5b81600560008673ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060003373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020016000205410156105aa576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004016105a190610ff5565b60405180910390fd5b81600460008673ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060008282546105f99190611044565b9250508190555081600460008573ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600082825461064f9190611078565b9250508190555081600560008673ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060003373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060008282546106e29190611044565b925050819055508273ffffffffffffffffffffffffffffffffffffffff168473ffffffffffffffffffffffffffffffffffffffff167fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef846040516107469190610ca7565b60405180910390a3600190509392505050565b600260009054906101000a900460ff1681565b6000600460008373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020549050919050565b600180546107c290610de8565b80601f01602080910402602001604051908101604052809291908181526020018280546107ee90610de8565b801561083b5780601f106108105761010080835404028352916020019161083b565b820191906000526020600020905b81548152906001019060200180831161081e57829003601f168201915b505050505081565b60008073ffffffffffffffffffffffffffffffffffffffff168373ffffffffffffffffffffffffffffffffffffffff16036108b3576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004016108aa90610f1d565b60405180910390fd5b81600460003373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020541015610935576040517f08c379a000000000000000000000000000000000000000000000000000000000815260040161092c90610f89565b60405180910390fd5b81600460003373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060008282546109849190611044565b9250508190555081600460008573ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060008282546109da9190611078565b925050819055508273ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff167fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef84604051610a3e9190610ca7565b60405180910390a36001905092915050565b6000600560008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060008373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002054905092915050565b600081519050919050565b600082825260208201905092915050565b60005b83811015610b11578082015181840152602081019050610af6565b60008484015250505050565b6000601f19601f8301169050919050565b6000610b3982610ad7565b610b438185610ae2565b9350610b53818560208601610af3565b610b5c81610b1d565b840191505092915050565b60006020820190508181036000830152610b818184610b2e565b905092915050565b600080fd5b600073ffffffffffffffffffffffffffffffffffffffff82169050919050565b6000610bb982610b8e565b9050919050565b610bc981610bae565b8114610bd457600080fd5b50565b600081359050610be681610bc0565b92915050565b6000819050919050565b610bff81610bec565b8114610c0a57600080fd5b50565b600081359050610c1c81610bf6565b92915050565b60008060408385031215610c3957610c38610b89565b5b6000610c4785828601610bd7565b9250506020610c5885828601610c0d565b9150509250929050565b60008115159050919050565b610c7781610c62565b82525050565b6000602082019050610c926000830184610c6e565b92915050565b610ca181610bec565b82525050565b6000602082019050610cbc6000830184610c98565b92915050565b600080600060608486031215610cdb57610cda610b89565b5b6000610ce986828701610bd7565b9350506020610cfa86828701610bd7565b9250506040610d0b86828701610c0d565b9150509250925092565b600060ff82169050919050565b610d2b81610d15565b82525050565b6000602082019050610d466000830184610d22565b92915050565b600060208284031215610d6257610d61610b89565b5b6000610d7084828501610bd7565b91505092915050565b60008060408385031215610d9057610d8f610b89565b5b6000610d9e85828601610bd7565b9250506020610daf85828601610bd7565b9150509250929050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052602260045260246000fd5b60006002820490506001821680610e0057607f821691505b602082108103610e1357610e12610db9565b5b50919050565b7f45524332303a207472616e736665722066726f6d20746865207a65726f20616460008201527f6472657373000000000000000000000000000000000000000000000000000000602082015250565b6000610e75602583610ae2565b9150610e8082610e19565b604082019050919050565b60006020820190508181036000830152610ea481610e68565b9050919050565b7f45524332303a207472616e7366657220746f20746865207a65726f206164647260008201527f6573730000000000000000000000000000000000000000000000000000000000602082015250565b6000610f07602383610ae2565b9150610f1282610eab565b604082019050919050565b60006020820190508181036000830152610f3681610efa565b9050919050565b7f45524332303a20696e73756666696369656e742062616c616e63650000000000600082015250565b6000610f73601b83610ae2565b9150610f7e82610f3d565b602082019050919050565b60006020820190508181036000830152610fa281610f66565b9050919050565b7f45524332303a20616c6c6f77616e636520657863656564656400000000000000600082015250565b6000610fdf601983610ae2565b9150610fea82610fa9565b602082019050919050565b6000602082019050818103600083015261100e81610fd2565b9050919050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b600061104f82610bec565b915061105a83610bec565b925082820390508181111561107257611071611015565b5b92915050565b600061108382610bec565b915061108e83610bec565b92508282019050808211156110a6576110a5611015565b5b9291505056fea2646970667358221220c38c26aa42c5f28ade4944774b361159e9cb76b6bb32a68ea063067e2204039764736f6c6343000817003300000000000000000000000000000000000000000000000000000000000001f4821333a0884842f1f22366fa9cf74f95190300e07c430d59e97bc70f304d83d618dde271a070573ea599065296539713b4ffe09bf003466de415cdc7c8307b41726c478fb6")
	if err != nil {
		log.Error("error encoding tx. Error: ", err)
	}
	tx := datastream.Transaction{
		L2BlockNumber:               1,
		Index:                       1,
		IsValid:                     true,
		Encoded:                     encode,
		EffectiveGasPricePercentage: 255, //nolint:mnd
		ImStateRoot:                 common.Hash{}.Bytes(),
	}
	dataTx, err := proto.Marshal(&tx)
	if err != nil {
		log.Error("error marshalling fake data TX. Ignoring it....")
	}
	return dataTx
}

func fakeDataBlockEnd(blockNum uint64) []byte {
	l2Block := datastream.L2BlockEnd{
		Number: 1,
	}
	l2B, err := proto.Marshal(&l2Block)
	if err != nil {
		log.Error("error marshalling fake blockEnd. Ignoring it....")
	}
	return l2B
}

func fakeDataBatchStart(BatchNum uint64) []byte {
	batchStart := datastream.BatchStart{
		Number:  1,
		Type:    1,    // Regular batch
		ForkId:  11,   //nolint:mnd
		ChainId: 1337, //nolint:mnd
	}
	bs, err := proto.Marshal(&batchStart)
	if err != nil {
		log.Error("error marshalling fake batchStart. Ignoring it....")
	}
	return bs
}

func fakeDataBatchEnd(BatchNum uint64) []byte {
	batchEnd := datastream.BatchEnd{
		Number:        1,
		LocalExitRoot: common.Hash{}.Bytes(),
		StateRoot:     common.Hash{}.Bytes(),
	}
	be, err := proto.Marshal(&batchEnd)
	if err != nil {
		log.Error("error marshalling fake batchEnd. Ignoring it....")
	}
	return be
}

// runClient runs a local datastream client and tests its features
func runClient(ctx *cli.Context) error {
	// Set log level
	logLevel := ctx.String("log")
	log.Init(log.Config{
		Environment: "development",
		Level:       logLevel,
		Outputs:     []string{"stdout"},
	})

	// Parameters
	server := ctx.String("server")
	if server == "" {
		return errors.New("bad/missing parameters")
	}
	from := ctx.String("from")
	fromBookmark := ctx.String("frombookmark")
	queryHeader := ctx.Bool("header")
	queryEntry := ctx.String("entry")
	queryBookmark := ctx.String("bookmark")
	sanityCheck := ctx.Bool("sanitycheck")
	bookmarkType := ctx.Int("bookmarktype")
	if bookmarkType < 0 || bookmarkType > 255 {
		return errors.New("bad bookmarktype parameter, must be between 0 and 255")
	}
	bookType := datastream.BookmarkType(bookmarkType)
	paramDumpBatch := ctx.String("dumpbatch")

	// Create client
	c, err := datastreamer.NewClient(server, StSequencer)
	if err != nil {
		return err
	}

	// Set process entry callback function
	if !sanityCheck {
		if paramDumpBatch != noneType {
			if from == "latest" {
				from = "0"
			}
			nDumpBatch, err := strconv.Atoi(paramDumpBatch)
			if err != nil {
				return err
			}
			dumpBatchNumber = uint64(nDumpBatch)

			c.SetProcessEntryFunc(doDumpBatchData)
		} else {
			c.SetProcessEntryFunc(printEntryNum)
		}
	} else {
		c.SetProcessEntryFunc(checkEntryBlockSanity)
	}

	// Start client (connect to the server)
	err = c.Start()
	if err != nil {
		return err
	}

	// Query file header information
	if queryHeader {
		header, err := c.ExecCommandGetHeader()
		if err != nil {
			log.Infof("Error: %v", err)
		} else {
			log.Infof("QUERY HEADER: TotalEntries[%d] TotalLength[%d] Version[%d] SystemID[%d]",
				header.TotalEntries, header.TotalLength, header.Version, header.SystemID)
		}
		return nil
	}

	// Query entry option
	if queryEntry != noneType {
		qEntry, err := strconv.Atoi(queryEntry)
		if err != nil {
			return err
		}
		entry, err := c.ExecCommandGetEntry(uint64(qEntry))
		if err != nil {
			log.Infof("Error: %v", err)
		} else {
			log.Infof("QUERY ENTRY %d: Entry[%d] Length[%d] Type[%d] Data[%v]",
				qEntry, entry.Number, entry.Length, entry.Type, entry.Data)
		}
		return nil
	}

	// Query bookmark option
	if queryBookmark != noneType {
		qBookmark, err := strconv.Atoi(queryBookmark)
		if err != nil {
			return err
		}
		bookmark := datastream.BookMark{
			Type:  bookType,
			Value: uint64(qBookmark),
		}
		qBook, err := proto.Marshal(&bookmark)
		if err != nil {
			log.Error("error marshalling fake bookmark. Ignoring it....")
		}
		entry, err := c.ExecCommandGetBookmark(qBook)
		if err != nil {
			log.Infof("Error: %v", err)
		} else {
			log.Infof("QUERY BOOKMARK (%d)%v: Entry[%d] Length[%d] Type[%d] Data[%v]",
				bookType, qBook, entry.Number, entry.Length, entry.Type, entry.Data)
		}
		return nil
	}

	// Command header: Get status
	header, err := c.ExecCommandGetHeader()
	if err != nil {
		return err
	}

	if fromBookmark != noneType {
		// Command StartBookmark: Sync and start streaming receive from bookmark
		fromBookNum, err := strconv.Atoi(fromBookmark)
		if err != nil {
			return err
		}
		bookmark := datastream.BookMark{
			Type:  bookType,
			Value: uint64(fromBookNum),
		}
		qBook, err := proto.Marshal(&bookmark)
		if err != nil {
			log.Error("error marshalling fake bookmark. Ignoring it....")
		}
		err = c.ExecCommandStartBookmark(qBook)
		if err != nil {
			return err
		}
	} else {
		// Command start: Sync and start streaming receive from entry number
		var fromEntry uint64
		if from == "latest" {
			fromEntry = header.TotalEntries
		} else {
			fromNum, err := strconv.Atoi(from)
			if err != nil {
				return err
			}
			fromEntry = uint64(fromNum)
		}
		err = c.ExecCommandStart(fromEntry)
		if err != nil {
			return err
		}
	}

	// After the initial sync, run until Ctl+C
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGTERM)
	<-interruptSignal

	// Command stop: Stop streaming
	err = c.ExecCommandStop()
	if err != nil {
		return err
	}

	log.Info("Client stopped")
	return nil
}

// printEntryNum prints basic data of the entry
func printEntryNum(e *datastreamer.FileEntry, c *datastreamer.StreamClient, s *datastreamer.StreamServer) error {
	log.Infof("PROCESS entry(%s): %d | %d | %d | %d", c.ID, e.Number, e.Length, e.Type, len(e.Data))
	return nil
}

// checkEntryBlockSanity checks entry, bookmark, and block sequence consistency
func checkEntryBlockSanity(
	e *datastreamer.FileEntry,
	c *datastreamer.StreamClient,
	s *datastreamer.StreamServer) error {
	// Sanity check initialization
	if !initSanityEntry {
		initSanityEntry = true
		if c.GetFromStream() > 0 {
			sanityEntry = c.GetFromStream()
		} else {
			sanityEntry = 0
		}
	}

	// Log work in progress
	if e.Number%100000 == 0 {
		log.Infof("Checking entry #%d...", e.Number)
	}

	// Sanity check for entry sequence
	if sanityEntry > 0 {
		if e.Number != sanityEntry {
			if e.Number < sanityEntry {
				log.Warnf("(X) SANITY CHECK failed: REPEATED entries? Received[%d] | Entry expected[%d]", e.Number, sanityEntry)
			} else {
				log.Warnf("(X) SANITY CHECK failed: GAP entries? Received[%d] | Entry expected[%d]", e.Number, sanityEntry)
			}
			return errors.New("sanity check failed for entry sequence")
		}
	} else {
		if e.Number != 0 {
			log.Warnf("(X) SANITY CHECK failed: Entry received[%d] | Entry expected[0]", e.Number)
			return errors.New("sanity check failed for entry sequence")
		}
	}
	sanityEntry++

	switch e.Type {
	case datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_BATCH_START):
		log.Debug("BATCH_START")
		batch := &datastream.BatchStart{}
		err := proto.Unmarshal(e.Data, batch)
		if err != nil {
			log.Error("error decoding batchStart. Error: ", err)
			return err
		}
		batchNum := batch.Number
		//Check previous End batch
		if sanityBatchEnd != batchNum {
			log.Warnf(`(X) SANITY CHECK failed (%d): BatchStart but the previous one is not closed yet? 
				lastBatchEnded[%d] Received[%d] | BatchStart expected[%d]`,
				e.Number, sanityBatchEnd-1, batchNum, sanityBatch)
		}
		// Check forkID
		if sanityForkID > batch.ForkId {
			log.Warnf("(X) SANITY CHECK failed (%d): Wrong ForkID for batch %d. ForkID received[%d] | ForkID expected[%d]",
				e.Number, batchNum, batch.ForkId, sanityForkID)
		}
		// Check batch number
		if sanityBatch > 0 {
			if batchNum != sanityBatch {
				if batchNum < sanityBatch {
					log.Warnf("(X) SANITY CHECK failed (%d): REPEATED batch? Received[%d] | Batch expected[%d]",
						e.Number, batchNum, sanityBatch)
				} else {
					log.Warnf("(X) SANITY CHECK failed (%d): GAP batch? Received[%d] | Batch expected[%d]",
						e.Number, batchNum, sanityBatch)
				}
				sanityBatch = batchNum
			}
		} else {
			if batchNum != 0 {
				if initSanityBatch {
					log.Warnf("(X) SANITY CHECK failed (%d): Batch received[%d] | Batch expected[0]", e.Number, batchNum)
					sanityBatch = 0
				} else {
					log.Infof("SANITY CHECK note (%d): First Batch received[%d]", e.Number, batchNum)
					sanityBatch = batchNum
				}
				initSanityBatch = true
			}
		}
		sanityBatch++
	case datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_L2_BLOCK):
		log.Debug("L2_BLOCK")
		l2Block := &datastream.L2Block{}
		err := proto.Unmarshal(e.Data, l2Block)
		if err != nil {
			log.Error("error decoding l2 block. Error: ", err)
			return err
		}
		blockNum := l2Block.Number
		log.Debug("L2BlockNum: ", blockNum)
		//Check previous End Block
		if sanityBlockEnd != blockNum {
			log.Warnf(`(X) SANITY CHECK failed (%d): BlockStart but the previous one is not closed yet? 
				lastBlockEnded[%d] Received[%d] | BlockStart expected[%d]`,
				e.Number, sanityBlockEnd-1, blockNum, sanityBlock)
		}
		if sanityBlock > 0 {
			if blockNum != sanityBlock {
				if blockNum < sanityBlock {
					log.Warnf("(X) SANITY CHECK failed (%d): REPEATED blocks? Received[%d] | Block expected[%d]",
						e.Number, blockNum, sanityBlock)
				} else {
					log.Warnf("(X) SANITY CHECK failed (%d): GAP blocks? Received[%d] | Block expected[%d]",
						e.Number, blockNum, sanityBlock)
				}
				sanityBlock = blockNum
			}
		} else {
			if blockNum != 0 {
				if initSanityBlock {
					log.Warnf("(X) SANITY CHECK failed (%d): Block received[%d] | Block expected[0]", e.Number, blockNum)
					sanityBlock = 0
				} else {
					log.Infof("SANITY CHECK note (%d): First Block received[%d]", e.Number, blockNum)
					sanityBlock = blockNum
				}
				initSanityBlock = true
			}
		}
		sanityBlock++
	case datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_TRANSACTION):
		log.Debug("TRANSACTION")
		dsTx := &datastream.Transaction{}
		err := proto.Unmarshal(e.Data, dsTx)
		if err != nil {
			log.Error("error decoding transaction. Error: ", err)
			return err
		}
		// If tx is well decoded is OK
	case datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_BATCH_END):
		log.Debug("BATCH_END")
		batch := &datastream.BatchEnd{}
		err := proto.Unmarshal(e.Data, batch)
		if err != nil {
			log.Error("error decoding batchEnd. Error: ", err)
			return err
		}
		batchNum := batch.Number
		//Check Open batch
		if sanityBatch-1 != sanityBatchEnd {
			log.Warnf(`(X) SANITY CHECK failed (%d): BatchEnd but not closed? 
				lastBatchOpened[%d] Received[%d] | BatchEnd expected[%d]`,
				e.Number, sanityBatch-1, batchNum, sanityBatchEnd)
		}
		// Check batch number
		if sanityBatchEnd > 0 {
			if batchNum != sanityBatchEnd {
				if batchNum < sanityBatchEnd {
					log.Warnf("(X) SANITY CHECK failed (%d): REPEATED batchEnd? Received[%d] | BatchEnd expected[%d]",
						e.Number, batchNum, sanityBatchEnd)
				} else {
					log.Warnf("(X) SANITY CHECK failed (%d): GAP batchEnd? Received[%d] | BatchEnd expected[%d]",
						e.Number, batchNum, sanityBatchEnd)
				}
				sanityBatchEnd = batchNum
			}
		} else {
			if batchNum != 0 {
				if initSanityBatchEnd {
					log.Warnf("(X) SANITY CHECK failed (%d): BatchEnd received[%d] | BatchEnd expected[0]", e.Number, batchNum)
					sanityBatchEnd = 0
				} else {
					log.Infof("SANITY CHECK note (%d): First BatchEnd received[%d]", e.Number, batchNum)
					sanityBatchEnd = batchNum
				}
				initSanityBatchEnd = true
			}
		}
		sanityBatchEnd++
	case datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_UPDATE_GER):
		log.Debug("UPDATE_GER")
		updateGer := &datastream.UpdateGER{}
		err := proto.Unmarshal(e.Data, updateGer)
		if err != nil {
			log.Error("error decoding updateGER. Error: ", err)
			return err
		}
		// If GER is well decoded is OK
	case datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_L2_BLOCK_END):
		log.Debug("L2_BLOCK_END")
		l2BlockEnd := &datastream.L2BlockEnd{}
		err := proto.Unmarshal(e.Data, l2BlockEnd)
		if err != nil {
			log.Error("error decoding l2BlockEnd. Error: ", err)
			return err
		}
		blockNum := l2BlockEnd.Number
		//Check Open l2 block
		if sanityBlock-1 != sanityBlockEnd {
			log.Warnf(`(X) SANITY CHECK failed (%d): BlockEnd but not closed? 
				lastBlockOpened[%d] Received[%d] | BlockEnd expected[%d]`,
				e.Number, sanityBlock-1, blockNum, sanityBlockEnd)
		}
		// Check l2 block end number
		if sanityBlockEnd > 0 {
			if blockNum != sanityBlockEnd {
				if blockNum < sanityBlockEnd {
					log.Warnf("(X) SANITY CHECK failed (%d): REPEATED blocks end? Received[%d] | Block expected[%d]",
						e.Number, blockNum, sanityBlockEnd)
				} else {
					log.Warnf("(X) SANITY CHECK failed (%d): GAP blocks end? Received[%d] | Block expected[%d]",
						e.Number, blockNum, sanityBlockEnd)
				}
				sanityBlockEnd = blockNum
			}
		} else {
			if blockNum != 0 {
				if initSanityBlockEnd {
					log.Warnf("(X) SANITY CHECK failed (%d): Block end received[%d] | Block expected[0]", e.Number, blockNum)
					sanityBlockEnd = 0
				} else {
					log.Infof("SANITY CHECK note (%d): First Block end received[%d]", e.Number, blockNum)
					sanityBlockEnd = blockNum
				}
				initSanityBlockEnd = true
			}
		}
		sanityBlockEnd++
	case datastreamer.EtBookmark: // Sanity check for bookmarks
		bookmark := &datastream.BookMark{}
		err := proto.Unmarshal(e.Data, bookmark)
		if err != nil {
			log.Error("error decoding bookmark. Error: ", err)
			return err
		}
		bookmarkNum := bookmark.Value

		switch bookmark.Type {
		case BookmarkBatch:
			log.Debug("BookmarkBatch")
			if sanityBookmarkBatch > 0 {
				if bookmarkNum != sanityBookmarkBatch {
					if bookmarkNum < sanityBookmarkBatch {
						log.Warnf("(X) SANITY CHECK failed (%d): REPEATED Batch bookmarks? Received[%d] | Bookmark expected[%d]",
							e.Number, bookmarkNum, sanityBookmarkBatch)
					} else {
						log.Warnf("(X) SANITY CHECK failed (%d): GAP Batch bookmarks? Received[%d] | Bookmark expected[%d]",
							e.Number, bookmarkNum, sanityBookmarkBatch)
					}
					sanityBookmarkBatch = bookmarkNum
				}
			} else {
				if bookmarkNum != 0 {
					if initSanityBookmark {
						log.Warnf("(X) SANITY CHECK failed (%d): Batch Bookmark received[%d] | Bookmark expected[0]",
							e.Number, bookmarkNum)
						sanityBookmarkBatch = 0
					} else {
						log.Infof("SANITY CHECK note (%d): First Batch Bookmark received[%d]", e.Number, bookmarkNum)
						sanityBookmarkBatch = bookmarkNum
					}
					initSanityBookmark = true
				}
			}
			sanityBookmarkBatch++
		case BookmarkL2Block:
			log.Debug("BookmarkL2Block")
			if sanityBookmarkL2Block > 0 {
				if bookmarkNum != sanityBookmarkL2Block {
					if bookmarkNum < sanityBookmarkL2Block {
						log.Warnf("(X) SANITY CHECK failed (%d): REPEATED L2block bookmarks? Received[%d] | Bookmark expected[%d]",
							e.Number, bookmarkNum, sanityBookmarkL2Block)
					} else {
						log.Warnf("(X) SANITY CHECK failed (%d): GAP L2block bookmarks? Received[%d] | Bookmark expected[%d]",
							e.Number, bookmarkNum, sanityBookmarkL2Block)
					}
					sanityBookmarkL2Block = bookmarkNum
				}
			} else {
				if bookmarkNum != 0 {
					if initSanityBookmark {
						log.Warnf("(X) SANITY CHECK failed (%d): L2block Bookmark received[%d] | Bookmark expected[0]",
							e.Number, bookmarkNum)
						sanityBookmarkL2Block = 0
					} else {
						log.Infof("SANITY CHECK note (%d): First L2block Bookmark received[%d]", e.Number, bookmarkNum)
						sanityBookmarkL2Block = bookmarkNum
					}
					initSanityBookmark = true
				}
			}
			sanityBookmarkL2Block++
		}
	}

	// Sanity check end condition
	if e.Number+1 >= c.GetTotalEntries() {
		log.Infof(`SANITY CHECK finished! From entry [%d] to entry [%d]. 
			Latest L2block[%d], sanityBookmarkL2Block[%d], sanityBookmarkBatch[%d]`,
			c.GetFromStream(), c.GetTotalEntries()-1, sanityBlock-1, sanityBookmarkL2Block-1, sanityBookmarkBatch-1)
		os.Exit(0)
	}

	return nil
}

// doDumpBatchData performs a batch data dump
func doDumpBatchData(e *datastreamer.FileEntry, c *datastreamer.StreamClient, s *datastreamer.StreamServer) error {
	type BatchDump struct {
		Number     uint64 `json:"batchNumber"`
		EntryFirst uint64 `json:"entryFirst"`
		EntryLast  uint64 `json:"entryLast"`
		BlockFirst uint64 `json:"l2BlockFirst"`
		BlockLast  uint64 `json:"l2BlockLast"`
		TotalTx    uint64 `json:"totalTx"`
		Data       string `json:"batchData"`
	}

	if e.Type != datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_L2_BLOCK) &&
		e.Type != datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_TRANSACTION) &&
		e.Type != datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_L2_BLOCK_END) {
		return nil
	}

	// L2 block start
	if e.Type == datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_L2_BLOCK) {
		l2Block := &datastream.L2Block{}
		err := proto.Unmarshal(e.Data, l2Block)
		if err != nil {
			log.Error("error decoding l2 block. Error: ", err)
			return err
		}
		batchNumber := l2Block.BatchNumber
		switch {
		case batchNumber < dumpBatchNumber:
			return nil
		case (batchNumber > dumpBatchNumber) || (e.Number+1 >= c.GetTotalEntries()):
			log.Infof("DUMP BATCH finished! First entry[%d], last entry[%d], first block[%d], last block[%d], total tx[%d]",
				dumpEntryFirst, dumpEntryLast, dumpBlockFirst, dumpBlockLast, dumpTotalTx)

			// Dump to json file
			fileName := fmt.Sprintf("dumpbatch%d.json", dumpBatchNumber)
			file, err := os.Create(fileName)
			if err != nil {
				return errors.New("creating dump file")
			}
			defer file.Close()

			bd := BatchDump{
				Number:     dumpBatchNumber,
				EntryFirst: dumpEntryFirst,
				EntryLast:  dumpEntryLast,
				BlockFirst: dumpBlockFirst,
				BlockLast:  dumpBlockLast,
				TotalTx:    dumpTotalTx,
				Data:       dumpBatchData,
			}

			encoder := json.NewEncoder(file)
			err = encoder.Encode(bd)
			if err != nil {
				return errors.New("writing dump file")
			}

			os.Exit(0)

		case batchNumber == dumpBatchNumber:
			initDumpBatch = true

			blockNum := l2Block.Number
			if dumpBlockFirst == 0 {
				dumpBlockFirst = blockNum
			}
			dumpBlockLast = blockNum
		}
	} else if e.Type == datastreamer.EntryType(datastream.EntryType_ENTRY_TYPE_TRANSACTION) && initDumpBatch {
		dumpTotalTx++
	}

	// Add data
	if initDumpBatch {
		if dumpEntryFirst == 0 {
			dumpEntryFirst = e.Number
		}
		dumpEntryLast = e.Number

		dumpBatchData += fmt.Sprintf("%02x%08x%08x%016x%x", 2, e.Length, e.Type, e.Number, e.Data) //nolint:mnd
	}
	return nil
}

// runRelay runs a local datastream relay
func runRelay(ctx *cli.Context) error {
	// Set log level
	logLevel := ctx.String("log")
	log.Init(log.Config{
		Environment: "development",
		Level:       logLevel,
		Outputs:     []string{"stdout"},
	})

	log.Info(">> App begin")

	// Parameters
	server := ctx.String("server")
	port := ctx.Uint64("port")
	file := ctx.String("file")
	if server == "" || file == "" || port <= 0 {
		return errors.New("bad/missing parameters")
	}
	writeTimeout := ctx.Uint64("writetimeout")
	inactivityTimeout := ctx.Uint64("inactivitytimeout")

	// Create relay server
	r, err := datastreamer.NewRelay(server, uint16(port), streamerVersion, streamerSystemID, StSequencer, file,
		time.Duration(writeTimeout)*time.Millisecond, time.Duration(inactivityTimeout)*time.Second,
		5*time.Second, nil) //nolint:mnd
	if err != nil {
		return err
	}

	// Start relay server
	err = r.Start()
	if err != nil {
		log.Error(">> App error! Start")
		return err
	}

	// Run until Ctl+C
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGTERM)
	<-interruptSignal

	log.Info(">> App end")
	return nil
}
