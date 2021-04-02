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
 
	 "github.com/minio/cli"
	 miniogo "github.com/minio/minio-go/v7"
	 "github.com/minio/minio-go/v7/pkg/credentials"
	 "github.com/minio/minio/pkg/console"
 )

 var moveCmd = cli.Command{
	 Name:   "move",
	 Usage:  "move objects up one level",
	 Action: moveAction,
	 Flags:  append(allFlags, migrateFlags...),
	 CustomHelpTemplate: `NAME:
	 {{.HelpName}} - {{.Usage}}
 
 USAGE:
	 {{.HelpName}} [--skip, --fake]
 
 FLAGS:
	{{range .VisibleFlags}}{{.}}
	{{end}}
 
 EXAMPLES:
 1. Move objects in "object_listing.txt" in MinIO.
	$ export MINIO_ENDPOINT=https://minio:9000
	$ export MINIO_ACCESS_KEY=minio
	$ export MINIO_SECRET_KEY=minio123
	$ export MINIO_BUCKET=miniobucket
	$ moveobject move --data-dir /tmp/
 
 2. Move objects in "object_listing.txt" in MinIO after skipping 100000 entries in this file
	$ export MINIO_ENDPOINT=https://minio:9000
	$ export MINIO_ACCESS_KEY=minio
	$ export MINIO_SECRET_KEY=minio123
	$ export MINIO_BUCKET=miniobucket
	$ moveobject move --data-dir /tmp/ --skip 10000
 
 3. Perform a dry run for moving objects in "object_listing.txt" in MinIO
	$ export MINIO_ENDPOINT=https://minio:9000
	$ export MINIO_ACCESS_KEY=minio
	$ export MINIO_SECRET_KEY=minio123
	$ export MINIO_BUCKET=miniobucket
	$ moveobject move --data-dir /tmp/ --fake --log
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
		 mvState.queueUploadTask(o)
		 logDMsg(fmt.Sprintf("adding %s to migration queue", o), nil)
	 }
	 if err := scanner.Err(); err != nil {
		 logDMsg(fmt.Sprintf("error processing file :%s ", objListFile), err)
		 return err
	 }
	 mvState.finish(ctx)
	 logMsg("successfully completed migration.")
 
	 return nil
 }
 