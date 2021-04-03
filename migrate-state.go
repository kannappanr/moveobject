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

const (
	objListFile       = "object_listing.txt"
	failMigFile       = "migration_fails.txt"
	failMoveFile      = "move_fails.txt"
	failCopyFile      = "copy_fails.txt"
	failDeleteFile    = "delete_fails.txt"
	successMigFile    = "migration_success.txt"
	successMoveFile   = "move_success.txt"
	successCopyFile   = "copy_success.txt"
	successDeleteFile = "delete_success.txt"
)

var dryRun bool

type migrateState struct {
	objectCh  chan string
	failedCh  chan string
	successCh chan string
	count     uint64
	failCnt   uint64
	wg        sync.WaitGroup
}

func (m *migrateState) queueUploadTask(obj string) {
	m.objectCh <- obj
}

var (
	migrationState      *migrateState
	migrationConcurrent = 100
)

func newMigrationState(ctx context.Context) *migrateState {
	if runtime.GOMAXPROCS(0) > migrationConcurrent {
		migrationConcurrent = runtime.GOMAXPROCS(0)
	}
	ms := &migrateState{
		objectCh:  make(chan string, migrationConcurrent),
		failedCh:  make(chan string, migrationConcurrent),
		successCh: make(chan string, migrationConcurrent),
	}

	return ms
}

// Increase count processed
func (m *migrateState) incCount() {
	atomic.AddUint64(&m.count, 1)
}

// Get total count processed
func (m *migrateState) getCount() uint64 {
	return atomic.LoadUint64(&m.count)
}

// Increase count failed
func (m *migrateState) incFailCount() {
	atomic.AddUint64(&m.failCnt, 1)
}

// Get total count failed
func (m *migrateState) getFailCount() uint64 {
	return atomic.LoadUint64(&m.failCnt)
}

// addWorker creates a new worker to process tasks
func (m *migrateState) addWorker(ctx context.Context) {
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
				logDMsg(fmt.Sprintf("Migrating...%s", obj), nil)
				if err := migrateObject(ctx, obj); err != nil {
					m.incFailCount()
					logMsg(fmt.Sprintf("error migrating object %s: %s", obj, err))
					m.failedCh <- obj
					continue
				}
				m.successCh <- obj
				m.incCount()
			}
		}
	}()
}
func (m *migrateState) finish(ctx context.Context) {
	close(m.objectCh)
	m.wg.Wait() // wait on workers to finish
	close(m.failedCh)

	if !dryRun {
		logMsg(fmt.Sprintf("Migrated %d objects, %d failures", m.getCount(), m.getFailCount()))
	}
}
func (m *migrateState) init(ctx context.Context) {
	if m == nil {
		return
	}
	for i := 0; i < migrationConcurrent; i++ {
		m.addWorker(ctx)
	}
	go func() {
		f, err := os.OpenFile(path.Join(dirPath, failMigFile), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			logDMsg("could not create + failMigFile", err)
			return
		}
		fwriter := bufio.NewWriter(f)
		defer fwriter.Flush()
		defer f.Close()

		s, err := os.OpenFile(path.Join(dirPath, successMigFile), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			logDMsg("could not create "+successMigFile, err)
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
					logMsg(fmt.Sprintf("Error writing to migration_fails.txt for "+obj, err))
					os.Exit(1)
				}
			case obj, ok := <-m.successCh:
				if !ok {
					return
				}
				if _, err := s.WriteString(obj + "\n"); err != nil {
					logMsg(fmt.Sprintf("Error writing to migration_success.txt for "+obj, err))
					os.Exit(1)
				}
			}
		}
	}()
}

func migrateObject(ctx context.Context, object string) error {
	r, err := minioSrcClient.GetObject(ctx, minioSrcBucket, object, miniogo.GetObjectOptions{})
	if err != nil {
		return err
	}

	stat, err := r.Stat()
	if err != nil {
		fmt.Println(err)
		logMsg(migrateMsg(object, convert(object)))
		return nil
	}
	defer r.Close()
	if dryRun {
		logMsg(migrateMsg(object, convert(object)))
		return nil
	}
	_, err = minioClient.PutObject(ctx, minioBucket, convert(object), r, stat.Size, miniogo.PutObjectOptions{})
	if err != nil {
		logDMsg("upload to minio client failed for "+object, err)
		return err
	}
	logDMsg("Uploaded "+object+" successfully", nil)
	return nil
}
