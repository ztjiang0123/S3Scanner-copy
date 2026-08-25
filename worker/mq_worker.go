package worker

import (
	"encoding/json"
	"fmt"
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/db"
	"github.com/sa7mon/s3scanner/mq"
	"github.com/sa7mon/s3scanner/provider"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"os"
	"sync"
)

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func WorkMQ(threadID int, wg *sync.WaitGroup, conn *amqp.Connection, provider provider.StorageProvider, queue string,
	threads int, doEnumerate bool, writeToDB bool) {
	_, once := os.LookupEnv("TEST_MQ") // If we're being tested, exit after one bucket is scanned
	defer wg.Done()

	// Wrap the whole thing in a for (while) loop so if the mq server kills the channel, we start it up again
	for {
		ch, chErr := mq.Connect(conn, queue, threads, threadID)
		if chErr != nil {
			FailOnError(chErr, "couldn't connect to message queue")
		}

		msgs, consumeErr := ch.Consume(queue, fmt.Sprintf("%s_%v", queue, threadID), false, false, false, false, nil)
		if consumeErr != nil {
			log.Error(fmt.Errorf("failed to register a consumer: %w", consumeErr))
			return
		}

		for j := range msgs {
			outcome := processMessage(j, provider, doEnumerate, writeToDB)
			if outcome == msgChannelClosed {
				// The server likely closed the channel; break to the top of the
				// outer for-loop to re-establish a new one.
				break
			}
			if outcome == msgDone && once {
				return
			}
		}
	}
}

// msgOutcome describes how the outer WorkMQ loop should react after a message
// has been handled.
type msgOutcome int

const (
	// msgHandled means the message was processed (or skipped); continue with the
	// next message on the same channel.
	msgHandled msgOutcome = iota
	// msgDone means a bucket was fully scanned and acked; identical to
	// msgHandled except it lets the caller honor the one-shot TEST_MQ mode.
	msgDone
	// msgChannelClosed means acking failed, signaling the channel is gone and
	// must be re-established.
	msgChannelClosed
)

// storeBucket persists b to the database when writeToDB is set, logging any error.
func storeBucket(b *bucket.Bucket, writeToDB bool) {
	if !writeToDB {
		return
	}
	if dbErr := db.StoreBucket(b); dbErr != nil {
		log.Error(dbErr)
	}
}

// processMessage validates, scans, optionally enumerates, and persists a single
// bucket delivered over the message queue. It returns an msgOutcome describing
// how the caller should proceed.
func processMessage(j amqp.Delivery, provider provider.StorageProvider, doEnumerate bool, writeToDB bool) msgOutcome {
	bucketToScan := bucket.Bucket{}
	if unmarshalErr := json.Unmarshal(j.Body, &bucketToScan); unmarshalErr != nil {
		log.Error(unmarshalErr)
	}

	if !bucket.IsValidS3BucketName(bucketToScan.Name) {
		log.Info(fmt.Sprintf("invalid   | %s", bucketToScan.Name))
		FailOnError(j.Ack(false), "failed to ack")
		return msgHandled
	}

	b, existsErr := provider.BucketExists(&bucketToScan)
	if existsErr != nil {
		log.WithFields(log.Fields{"bucket": b.Name, "step": "checkExists"}).Error(existsErr)
		FailOnError(j.Reject(false), "failed to reject")
	}
	if b.Exists == bucket.BucketNotExist {
		// ack the message and skip to the next
		log.Infof("not_exist | %s", b.Name)
		FailOnError(j.Ack(false), "failed to ack")
		return msgHandled
	}

	if scanErr := provider.Scan(b, false); scanErr != nil {
		log.WithFields(log.Fields{"bucket": b}).Error(scanErr)
		FailOnError(j.Reject(false), "failed to reject")
		return msgHandled
	}

	if doEnumerate {
		if handled := enumerateBucket(j, provider, b, &bucketToScan, writeToDB); handled {
			return msgHandled
		}
	}

	PrintResult(&bucketToScan, false)
	if ackErr := j.Ack(false); ackErr != nil {
		// Acknowledge mq message. May fail if we've taken too long and the server has closed the channel
		log.WithFields(log.Fields{"bucket": b}).Error(ackErr)
		return msgChannelClosed
	}

	storeBucket(&bucketToScan, writeToDB)
	return msgDone
}

// enumerateBucket enumerates the objects of a readable bucket. It returns true
// when it has fully handled the message (acked and stored), meaning the caller
// should stop processing it further.
func enumerateBucket(j amqp.Delivery, provider provider.StorageProvider, b *bucket.Bucket, bucketToScan *bucket.Bucket, writeToDB bool) bool {
	if b.PermAllUsersRead != bucket.PermissionAllowed {
		PrintResult(bucketToScan, false)
		FailOnError(j.Ack(false), "failed to ack")
		storeBucket(bucketToScan, writeToDB)
		return true
	}

	log.WithFields(log.Fields{"method": "main.mqwork()",
		"bucket_name": b.Name, "region": b.Region}).Debugf("enumerating objects...")

	if enumErr := provider.Enumerate(b); enumErr != nil {
		log.Errorf("Error enumerating bucket '%s': %v\nEnumerated objects: %v", b.Name, enumErr, len(b.Objects))
		FailOnError(j.Reject(false), "failed to reject")
	}
	return false
}
