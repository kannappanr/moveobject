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
	"time"

	miniogo "github.com/minio/minio-go/v7"
)

type deleteState struct {
	objectCh  chan string
	failedCh  chan string
	successCh chan string
	count     uint64
	failCnt   uint64
	wg        sync.WaitGroup
}

func (m *deleteState) queueUploadTask(obj string) {
	m.objectCh <- obj
}

var (
	delState         *deleteState
	deleteConcurrent = 100
)

func newDeleteState(ctx context.Context) *deleteState {
	if runtime.GOMAXPROCS(0) > deleteConcurrent {
		deleteConcurrent = runtime.GOMAXPROCS(0)
	}
	ms := &deleteState{
		objectCh:  make(chan string, deleteConcurrent),
		failedCh:  make(chan string, deleteConcurrent),
		successCh: make(chan string, deleteConcurrent),
	}

	return ms
}

// Increase count processed
func (m *deleteState) incCount() {
	atomic.AddUint64(&m.count, 1)
}

// Get total count processed
func (m *deleteState) getCount() uint64 {
	return atomic.LoadUint64(&m.count)
}

// Increase count failed
func (m *deleteState) incFailCount() {
	atomic.AddUint64(&m.failCnt, 1)
}

// Get total count failed
func (m *deleteState) getFailCount() uint64 {
	return atomic.LoadUint64(&m.failCnt)
}

// addWorker creates a new worker to process tasks
func (m *deleteState) addWorker(ctx context.Context) {
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
				if err := deleteObject(ctx, obj); err != nil {
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

func (m *deleteState) finish(ctx context.Context) {
	time.Sleep(100 * time.Millisecond)
	close(m.objectCh)
	m.wg.Wait() // wait on workers to finish
	close(m.failedCh)
	close(m.successCh)

	if !dryRun {
		logMsg(fmt.Sprintf("Moved %d objects, %d failures", m.getCount(), m.getFailCount()))
	}
}
func (m *deleteState) init(ctx context.Context) {
	if m == nil {
		return
	}
	for i := 0; i < deleteConcurrent; i++ {
		m.addWorker(ctx)
	}
	go func() {
		f, err := os.OpenFile(path.Join(dirPath, failDeleteFile+time.Now().Format(".01-02-2006-15-04-05")), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			logDMsg("could not create "+failDeleteFile, err)
			return
		}
		fwriter := bufio.NewWriter(f)
		defer fwriter.Flush()
		defer f.Close()

		s, err := os.OpenFile(path.Join(dirPath, successDeleteFile+time.Now().Format(".01-02-2006-15-04-05")), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			logDMsg("could not create "+successDeleteFile, err)
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
					logMsg(fmt.Sprintf("Error writing to copy_successs.txt for "+obj, err))
					os.Exit(1)
				}

			}
		}
	}()
}

func deleteObject(ctx context.Context, object string) error {
	stat, err := minioClient.StatObject(ctx, minioBucket, object, miniogo.StatObjectOptions{})
	if err != nil {
		return err
	}

	if dryRun {
		logMsg(migrateMsg(object, object))
		return nil
	}

	opts := miniogo.RemoveObjectOptions{
		VersionID: stat.VersionID,
	}

	err = minioClient.RemoveObject(ctx, minioBucket, object, opts)
	if err != nil {
		logDMsg("removeObject failed for "+object, err)
		return err
	}
	logDMsg("Removed "+object+" successfully", nil)
	return nil
}
