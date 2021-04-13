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
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/minio/cli"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/pkg/console"
)

var moveFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "start",
		Usage: "start of numbered prefix",
		Value: 0,
	},
	cli.IntFlag{
		Name:  "end",
		Usage: "end of numbered prefix",
		Value: 999,
	},
	cli.BoolFlag{
		Name:  "fake",
		Usage: "perform a fake migration",
	},
}

var moveCmd = cli.Command{
	Name:   "move",
	Usage:  "move objects up one level",
	Action: moveAction,
	Flags:  append(allFlags, moveFlags...),
	CustomHelpTemplate: `NAME:
	 {{.HelpName}} - {{.Usage}}
 
 USAGE:
	 {{.HelpName}} [--skip, --fake]
 
 FLAGS:
	{{range .VisibleFlags}}{{.}}
	{{end}}
 
 EXAMPLES:
 1. Move objects in MinIO with starting prefix of 0 to ending prefix of 99.
	$ export MINIO_ENDPOINT=https://minio:9000
	$ export MINIO_ACCESS_KEY=minio
	$ export MINIO_SECRET_KEY=minio123
	$ export MINIO_BUCKET=miniobucket
	$ moveobject move --data-dir /tmp/ --start 0 --end 99
  
 2. Perform a dry run for moving objects with starting prefix of 40 to ending prefix of 99.
	$ export MINIO_ENDPOINT=https://minio:9000
	$ export MINIO_ACCESS_KEY=minio
	$ export MINIO_SECRET_KEY=minio123
	$ export MINIO_BUCKET=miniobucket
	$ moveobject move --data-dir /tmp/ --fake --log --start 40 --end 99
 `,
}

func initMinioClient(ctx *cli.Context) error {
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

	api, err := miniogo.New(target.Host, &options)
	if err != nil {
		console.Fatalln(err)
	}

	// Store the new api object.
	minioClient = api
	return nil
}

func moveAction(cliCtx *cli.Context) error {
	checkArgsAndInit(cliCtx)
	ctx := context.Background()
	logMsg("Init minio client..")
	if err := initMinioClient(cliCtx); err != nil {
		logDMsg("Unable to  initialize MinIO client, exiting...%w", err)
		cli.ShowCommandHelp(cliCtx, cliCtx.Command.Name) // last argument is exit code
		console.Fatalln(err)
	}
	mvState = newMoveState(ctx)
	mvState.init(ctx)
	startPrefix := cliCtx.Int("start")
	endPrefix := cliCtx.Int("end")
	dryRun = cliCtx.Bool("fake")
	for i := startPrefix; i <= endPrefix; i++ {
		prefix := strconv.Itoa(i) + "/"
		logMsg("Starting prefix " + prefix)
		opts := miniogo.ListObjectsOptions{
			WithVersions: true,
			Recursive:    true,
			Prefix:       prefix,
		}
		for object := range minioClient.ListObjects(context.Background(), minioBucket, opts) {
			if object.Err != nil {
				fmt.Println(object.Err)
				return object.Err
			}
			if !object.IsDeleteMarker && object.IsLatest && patternMatch(object.Key) {
				mvState.queueUploadTask(object.VersionID + "," + object.Key)
				logDMsg(fmt.Sprintf("adding %s to move queue", object.Key+" : "+object.VersionID), nil)
			}
		}
	}
	mvState.finish(ctx)
	logMsg("successfully completed move.")

	return nil
}
