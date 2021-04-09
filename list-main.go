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
	"fmt"
	"os"
	"path"

	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio/pkg/console"
)

var listCmd = cli.Command{
	Name:   "list",
	Usage:  "list objects and it's version",
	Action: listAction,
	Flags:  allFlags,
	CustomHelpTemplate: `NAME:
	 {{.HelpName}} - {{.Usage}}
 
 USAGE:
	 {{.HelpName}} [--skip, --fake]
 
 FLAGS:
	{{range .VisibleFlags}}{{.}}
	{{end}}
 
 EXAMPLES:
 1. save list of object versions in "version_listing.txt" in MinIO.
	$ export MINIO_ENDPOINT=https://minio:9000
	$ export MINIO_ACCESS_KEY=minio
	$ export MINIO_SECRET_KEY=minio123
	$ export MINIO_BUCKET=miniobucket
	$ moveobject list --data-dir /tmp/
 `,
}

func listAction(cliCtx *cli.Context) error {
	checkArgsAndInit(cliCtx)
	logMsg("Init minio client..")
	if err := initMinioClient(cliCtx); err != nil {
		logDMsg("Unable to  initialize MinIO client, exiting...%w", err)
		cli.ShowCommandHelp(cliCtx, cliCtx.Command.Name) // last argument is exit code
		console.Fatalln(err)
	}
	s, err := os.OpenFile(path.Join(dirPath, versionListFile), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		logDMsg("could not create "+versionListFile, err)
		console.Fatalln(err)
	}
	swriter := bufio.NewWriter(s)
	defer swriter.Flush()
	defer s.Close()

	opts := minio.ListObjectsOptions{
		WithVersions: true,
		Recursive:    true,
		Prefix:       "",
	}

	// List all objects from a bucket-name with a matching prefix.
	for object := range minioClient.ListObjects(context.Background(), minioBucket, opts) {
		if object.Err != nil {
			fmt.Println(object.Err)
			return object.Err
		}
		if !object.IsDeleteMarker && object.IsLatest && patternMatch(object.Key) {
			if _, err := s.WriteString(object.VersionID + "," + object.Key + "\n"); err != nil {
				logMsg(fmt.Sprintf("Error writing to version_listing.txt for "+object.Key, err))
				os.Exit(1)
			}
		}
	}

	logMsg("successfully completed listing.")

	return nil
}
