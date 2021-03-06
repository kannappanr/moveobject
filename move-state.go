package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	miniogo "github.com/minio/minio-go/v7"
)

type moveState struct {
	objectCh  chan string
	failedCh  chan string
	successCh chan string
	count     uint64
	failCnt   uint64
	wg        sync.WaitGroup
}

func (m *moveState) queueUploadTask(obj string) {
	m.objectCh <- obj
}

var (
	mvState        *moveState
	moveConcurrent = 100
)

func newMoveState(ctx context.Context) *moveState {
	if runtime.GOMAXPROCS(0) > moveConcurrent {
		moveConcurrent = runtime.GOMAXPROCS(0)
	}
	ms := &moveState{
		objectCh:  make(chan string, moveConcurrent),
		failedCh:  make(chan string, moveConcurrent),
		successCh: make(chan string, moveConcurrent),
	}

	return ms
}

// Increase count processed
func (m *moveState) incCount() {
	atomic.AddUint64(&m.count, 1)
}

// Get total count processed
func (m *moveState) getCount() uint64 {
	return atomic.LoadUint64(&m.count)
}

// Increase count failed
func (m *moveState) incFailCount() {
	atomic.AddUint64(&m.failCnt, 1)
}

// Get total count failed
func (m *moveState) getFailCount() uint64 {
	return atomic.LoadUint64(&m.failCnt)
}

// addWorker creates a new worker to process tasks
func (m *moveState) addWorker(ctx context.Context) {
	m.wg.Add(1)
	// Add a new worker.
	go func() {
		defer m.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case object, ok := <-m.objectCh:
				if !ok {
					return
				}
				result := strings.SplitN(object, ",", 2)
				obj := result[1]
				versionID := result[0]
				logDMsg(fmt.Sprintf("Moving...%s", obj), nil)
				if !patternMatch(obj) {
					m.incFailCount()
					logMsg(fmt.Sprintf("error matching object %s", obj))
					m.failedCh <- obj
					continue
				}
				if err := moveObject(ctx, obj, versionID); err != nil {
					m.incFailCount()
					logMsg(fmt.Sprintf("error moving object %s: %s", obj, err))
					m.failedCh <- obj
					continue
				}
				m.successCh <- obj
				m.incCount()
			}
		}
	}()
}

func (m *moveState) finish(ctx context.Context) {
	time.Sleep(100 * time.Millisecond)
	close(m.objectCh)
	m.wg.Wait() // wait on workers to finish
	close(m.failedCh)
	close(m.successCh)

	if !dryRun {
		logMsg(fmt.Sprintf("Moved %d objects, %d failures", m.getCount(), m.getFailCount()))
	}
}
func (m *moveState) init(ctx context.Context) {
	if m == nil {
		return
	}
	for i := 0; i < moveConcurrent; i++ {
		m.addWorker(ctx)
	}
	go func() {
		f, err := os.OpenFile(path.Join(dirPath, failMoveFile+time.Now().Format(".01-02-2006-15-04-05")), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			logDMsg("could not create "+failMoveFile, err)
			return
		}
		fwriter := bufio.NewWriter(f)
		defer fwriter.Flush()
		defer f.Close()

		s, err := os.OpenFile(path.Join(dirPath, successMoveFile+time.Now().Format(".01-02-2006-15-04-05")), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			logDMsg("could not create "+successMoveFile, err)
			return
		}
		swriter := bufio.NewWriter(s)
		defer swriter.Flush()
		defer s.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case obj, ok := <-m.failedCh:
				if !ok {
					return
				}
				if _, err := f.WriteString(obj + "\n"); err != nil {
					logMsg(fmt.Sprintf("Error writing to move_fails.txt for "+obj, err))
					os.Exit(1)
				}
			case obj, ok := <-m.successCh:
				if !ok {
					return
				}
				if _, err := s.WriteString(obj + "\n"); err != nil {
					logMsg(fmt.Sprintf("Error writing to move_success.txt for "+obj, err))
					os.Exit(1)
				}
			}
		}
	}()
}

func moveObject(ctx context.Context, object, versionID string) error {
	if dryRun {
		logMsg(migrateMsg(object, object))
		return nil
	}

	src := miniogo.CopySrcOptions{
		Bucket:    minioBucket,
		Object:    object,
		VersionID: versionID,
	}

	// Destination object
	dst := miniogo.CopyDestOptions{
		Bucket: minioBucket,
		Object: convert(object),
	}

	_, err := minioClient.CopyObject(ctx, dst, src)
	if err != nil {
		logDMsg("upload to minio client failed for "+object, err)
		return err
	}
	opts := miniogo.RemoveObjectOptions{
		VersionID: versionID,
	}

	err = minioClient.RemoveObject(ctx, minioBucket, object, opts)
	if err != nil {
		logDMsg("removeObject failed for "+object, err)
		return err
	}
	logDMsg("Uploaded "+object+" successfully", nil)
	return nil
}
