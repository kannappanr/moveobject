/*
 * MinIO Client (C) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/fatih/color"
	"github.com/minio/cli"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/pkg/console"
)

var migrateFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "skip, s",
		Usage: "number of entries to skip from input file",
		Value: 0,
	},
	cli.BoolFlag{
		Name:  "fake",
		Usage: "perform a fake migration",
	},
}
var migrateCmd = cli.Command{
	Name:   "migrate",
	Usage:  "copy objects from one MinIO to another",
	Action: migrateAction,
	Flags:  append(allFlags, migrateFlags...),
	CustomHelpTemplate: `NAME:
	{{.HelpName}} - {{.Usage}}

USAGE:
	{{.HelpName}} [--skip, --fake]

FLAGS:
   {{range .VisibleFlags}}{{.}}
   {{end}}

EXAMPLES:
1. Migrate objects in "object_listing.txt" to MinIO.
   $ export MINIO_ENDPOINT=https://minio:9000
   $ export MINIO_ACCESS_KEY=minio
   $ export MINIO_SECRET_KEY=minio123
   $ export MINIO_SOURCE_ENDPOINT=https://minio-src:9000
   $ export MINIO_SOURCE_ACCESS_KEY=minio
   $ export MINIO_SOURCE_SECRET_KEY=minio123
   $ export MINIO_BUCKET=miniobucket
   $ moveobject migrate --data-dir /tmp/ 

2. Migrate objects in "object_listing.txt" from one MinIO tp another after skipping 100000 entries in this file
   $ export MINIO_ENDPOINT=https://minio:9000
   $ export MINIO_ACCESS_KEY=minio
   $ export MINIO_SECRET_KEY=minio123
   $ export MINIO_SOURCE_ENDPOINT=https://minio-src:9000
   $ export MINIO_SOURCE_ACCESS_KEY=minio
   $ export MINIO_SOURCE_SECRET_KEY=minio123
   $ export MINIO_BUCKET=miniobucket
   $ moveobject migrate --data-dir /tmp/ --skip 10000

3. Perform a dry run for migrating objects in "object_listing.txt" from one MinIO to another
   $ export MINIO_ENDPOINT=https://minio:9000
   $ export MINIO_ACCESS_KEY=minio
   $ export MINIO_SECRET_KEY=minio123
   $ export MINIO_SOURCE_ENDPOINT=https://minio-src:9000
   $ export MINIO_SOURCE_ACCESS_KEY=minio
   $ export MINIO_SOURCE_SECRET_KEY=minio123
   $ export MINIO_BUCKET=miniobucket
   $ moveobject migrate --data-dir /tmp/ --fake --log
`,
}
var minioClient *miniogo.Client
var minioSrcClient *miniogo.Client

const (

	// EnvMinIOEndpoint MinIO endpoint
	EnvMinIOEndpoint = "MINIO_ENDPOINT"
	// EnvMinIOAccessKey MinIO access key
	EnvMinIOAccessKey = "MINIO_ACCESS_KEY"
	// EnvMinIOSecretKey MinIO secret key
	EnvMinIOSecretKey = "MINIO_SECRET_KEY"

	// EnvMinIOSourceEndpoint MinIO endpoint
	EnvMinIOSourceEndpoint = "MINIO_SOURCE_ENDPOINT"
	// EnvMinIOSourceAccessKey MinIO access key
	EnvMinIOSourceAccessKey = "MINIO_SOURCE_ACCESS_KEY"
	// EnvMinIOSourceSecretKey MinIO secret key
	EnvMinIOSourceSecretKey = "MINIO_SOURCE_SECRET_KEY"

	// EnvMinIOBucket bucket to MinIO to.
	EnvMinIOBucket = "MINIO_BUCKET"
)
func checkArgsAndInit(ctx *cli.Context) {
	debugFlag = ctx.Bool("debug")
	logFlag = ctx.Bool("log")

	dirPath = ctx.String("data-dir")

	if dirPath == "" {
		console.Fatalln(fmt.Errorf("path to working dir required, please set --data-dir flag"))
		return
	}

	console.SetColor("Request", color.New(color.FgCyan))
	console.SetColor("Method", color.New(color.Bold, color.FgWhite))
	console.SetColor("Host", color.New(color.Bold, color.FgGreen))
	console.SetColor("ReqHeaderKey", color.New(color.Bold, color.FgWhite))
	console.SetColor("RespHeaderKey", color.New(color.Bold, color.FgCyan))
	console.SetColor("RespStatus", color.New(color.Bold, color.FgYellow))
	console.SetColor("ErrStatus", color.New(color.Bold, color.FgRed))
	console.SetColor("Response", color.New(color.FgGreen))
}


func initMinioClients(ctx *cli.Context) error {
	mURL := os.Getenv(EnvMinIOEndpoint)
	if mURL == "" {
		return fmt.Errorf("MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY and MINIO_BUCKET need to be set")
	}
	target, err := url.Parse(mURL)
	if err != nil {
		return fmt.Errorf("unable to parse input arg %s: %v", mURL, err)
	}

	accessKey := os.Getenv(EnvMinIOAccessKey)
	secretKey := os.Getenv(EnvMinIOSecretKey)
	minioBucket = os.Getenv(EnvMinIOBucket)

	if accessKey == "" || secretKey == "" || minioBucket == "" {
		console.Fatalln(fmt.Errorf("one or more of AccessKey:%s SecretKey: %s Bucket:%s ", accessKey, secretKey, minioBucket), "are missing in MinIO configuration")
	}

	srcAccessKey := os.Getenv(EnvMinIOSourceAccessKey)
	srcSecretKey := os.Getenv(EnvMinIOSourceSecretKey)
	srcEndpoint  := os.Getenv(EnvMinIOSourceEndpoint)

	if srcAccessKey == "" || srcEndpoint == "" || srcSecretKey == "" {
		console.Fatalln(fmt.Errorf("one or more of Source's AccessKey:%s SecretKey: %s Endpoint:%s ", srcAccessKey, srcSecretKey, srcEndpoint), "are missing in MinIO configuration")
	}

	src, err := url.Parse(srcEndpoint)
	if err != nil {
		return fmt.Errorf("unable to parse input arg %s: %v", srcEndpoint, err)
	}

	options := miniogo.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: target.Scheme == "https",
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          256,
			MaxIdleConnsPerHost:   16,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 10 * time.Second,
			TLSClientConfig: &tls.Config{
				RootCAs: mustGetSystemCertPool(),
				// Can't use SSLv3 because of POODLE and BEAST
				// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
				// Can't use TLSv1.1 because of RC4 cipher usage
				MinVersion:         tls.VersionTLS12,
				NextProtos:         []string{"http/1.1"},
				InsecureSkipVerify: ctx.GlobalBool("insecure"),
			},
			// Set this value so that the underlying transport round-tripper
			// doesn't try to auto decode the body of objects with
			// content-encoding set to `gzip`.
			//
			// Refer:
			//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
			DisableCompression: true,
		},
		Region:       "us-east-1",
		BucketLookup: 0,
	}

	minioClient, err = miniogo.New(target.Host, &options)
	if err != nil {
		console.Fatalln(err)
	}

	srcOptions := miniogo.Options{
		Creds:  credentials.NewStaticV4(srcAccessKey, srcSecretKey, ""),
		Secure: src.Scheme == "https",
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          256,
			MaxIdleConnsPerHost:   16,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 10 * time.Second,
			TLSClientConfig: &tls.Config{
				RootCAs: mustGetSystemCertPool(),
				MinVersion:         tls.VersionTLS12,
				NextProtos:         []string{"http/1.1"},
				InsecureSkipVerify: ctx.GlobalBool("insecure"),
			},
			DisableCompression: true,
		},
		Region:       "us-east-1",
		BucketLookup: 0,
	}

	minioSrcClient, err = miniogo.New(src.Host, &srcOptions)
	if err != nil {
		console.Fatalln(err)
	}
	return nil
}

func migrateAction(cliCtx *cli.Context) error {
	checkArgsAndInit(cliCtx)
	ctx := context.Background()
	logMsg("Init minio client..")
	if err := initMinioClients(cliCtx); err != nil {
		logDMsg("Unable to  initialize MinIO client, exiting...%w", err)
		cli.ShowCommandHelp(cliCtx, cliCtx.Command.Name) // last argument is exit code
		console.Fatalln(err)
	}
	migrationState = newMigrationState(ctx)
	migrationState.init(ctx)
	skip := cliCtx.Int("skip")
	dryRun = cliCtx.Bool("fake")

	file, err := os.Open(path.Join(dirPath, objListFile))
	if err != nil {
		logDMsg(fmt.Sprintf("could not open file :%s ", objListFile), err)
		return err
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		o := scanner.Text()
		if skip > 0 {
			skip--
			continue
		}
		migrationState.queueUploadTask(o)
		logDMsg(fmt.Sprintf("adding %s to migration queue", o), nil)
	}
	if err := scanner.Err(); err != nil {
		logDMsg(fmt.Sprintf("error processing file :%s ", objListFile), err)
		return err
	}
	migrationState.finish(ctx)
	logMsg("successfully completed migration.")

	return nil
}