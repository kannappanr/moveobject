package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path"
	"runtime"
	"sync"
	"sync/atomic"

	miniogo "github.com/minio/minio-go/v7"
)

type copyState struct {
	objectCh chan string
	failedCh chan string
	count    uint64
	failCnt  uint64
	wg       sync.WaitGroup
}

func (m *copyState) queueUploadTask(obj string) {
	m.objectCh <- obj
}

var (
	cpState        *copyState
	copyConcurrent = 100
)

func newCopyState(ctx context.Context) *copyState {
	if runtime.GOMAXPROCS(0) > copyConcurrent {
		copyConcurrent = runtime.GOMAXPROCS(0)
	}
	cp := &copyState{
		objectCh: make(chan string, copyConcurrent),
		failedCh: make(chan string, copyConcurrent),
	}

	return cp
}

// Increase count processed
func (m *copyState) incCount() {
	atomic.AddUint64(&m.count, 1)
}

// Get total count processed
func (m *copyState) getCount() uint64 {
	return atomic.LoadUint64(&m.count)
}

// Increase count failed
func (m *copyState) incFailCount() {
	atomic.AddUint64(&m.failCnt, 1)
}

// Get total count failed
func (m *copyState) getFailCount() uint64 {
	return atomic.LoadUint64(&m.failCnt)
}

// addWorker creates a new worker to process tasks
func (m *copyState) addWorker(ctx context.Context) {
	m.wg.Add(1)
	// Add a new worker.
	go func() {
		defer m.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case obj, ok := <-m.objectCh:
				if !ok {
					return
				}
				logDMsg(fmt.Sprintf("Moving...%s", obj), nil)
				if !patternMatch(obj) {
					m.incFailCount()
					logMsg(fmt.Sprintf("error matching object %s", obj))
					m.failedCh <- obj
					continue
				}
				if err := copyObject(ctx, obj); err != nil {
					m.incFailCount()
					logMsg(fmt.Sprintf("error moving object %s: %s", obj, err))
					m.failedCh <- obj
					continue
				}
				m.incCount()
			}
		}
	}()
}

func (m *copyState) finish(ctx context.Context) {
	close(m.objectCh)
	m.wg.Wait() // wait on workers to finish
	close(m.failedCh)

	if !dryRun {
		logMsg(fmt.Sprintf("Moved %d objects, %d failures", m.getCount(), m.getFailCount()))
	}
}
func (m *copyState) init(ctx context.Context) {
	if m == nil {
		return
	}
	for i := 0; i < copyConcurrent; i++ {
		m.addWorker(ctx)
	}
	go func() {
		f, err := os.OpenFile(path.Join(dirPath, failCopyFile), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			logDMsg("could not create "+failCopyFile, err)
			return
		}
		fwriter := bufio.NewWriter(f)
		defer fwriter.Flush()
		defer f.Close()

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

			}
		}
	}()
}

func copyObject(ctx context.Context, object string) error {

	if dryRun {
		logMsg(migrateMsg(object, convert(object)))
		return nil
	}

	src := miniogo.CopySrcOptions{
		Bucket: minioBucket,
		Object: object,
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
	logDMsg("Uploaded "+object+" successfully", nil)
	return nil
}
