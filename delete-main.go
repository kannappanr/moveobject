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
	"github.com/minio/minio/pkg/console"
)

var delCmd = cli.Command{
	Name:   "delete",
	Usage:  "delete objects specified in the list",
	Action: deleteAction,
	Flags:  append(allFlags, migrateFlags...),
	CustomHelpTemplate: `NAME:
	 {{.HelpName}} - {{.Usage}}
 
 USAGE:
	 {{.HelpName}} [--skip, --fake]
 
 FLAGS:
	{{range .VisibleFlags}}{{.}}
	{{end}}
 
 EXAMPLES:
 1. Delete objects in "object_listing.txt" in MinIO.
	$ export MINIO_ENDPOINT=https://minio:9000
	$ export MINIO_ACCESS_KEY=minio
	$ export MINIO_SECRET_KEY=minio123
	$ export MINIO_BUCKET=miniobucket
	$ moveobject delete --data-dir /tmp/
 
 2. Delete objects in "object_listing.txt" in MinIO after skipping 100000 entries in this file
	$ export MINIO_ENDPOINT=https://minio:9000
	$ export MINIO_ACCESS_KEY=minio
	$ export MINIO_SECRET_KEY=minio123
	$ export MINIO_BUCKET=miniobucket
	$ moveobject delete --data-dir /tmp/ --skip 10000
 
 3. Perform a dry run for deleting objects in "object_listing.txt" in MinIO
	$ export MINIO_ENDPOINT=https://minio:9000
	$ export MINIO_ACCESS_KEY=minio
	$ export MINIO_SECRET_KEY=minio123
	$ export MINIO_BUCKET=miniobucket
	$ moveobject delete --data-dir /tmp/ --fake --log
 `,
}

func deleteAction(cliCtx *cli.Context) error {
	checkArgsAndInit(cliCtx)
	ctx := context.Background()
	logMsg("Init minio client..")
	if err := initMinioClient(cliCtx); err != nil {
		logDMsg("Unable to  initialize MinIO client, exiting...%w", err)
		cli.ShowCommandHelp(cliCtx, cliCtx.Command.Name) // last argument is exit code
		console.Fatalln(err)
	}
	delState = newDeleteState(ctx)
	delState.init(ctx)
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
		delState.queueUploadTask(o)
		logDMsg(fmt.Sprintf("adding %s to migration queue", o), nil)
	}
	if err := scanner.Err(); err != nil {
		logDMsg(fmt.Sprintf("error processing file :%s ", objListFile), err)
		return err
	}
	delState.finish(ctx)
	logMsg("successfully completed deletion.")

	return nil
}
