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
	"fmt"
	"os"

	"github.com/minio/cli"
)

var (
	dirPath            string
	minioBucket        string
	minioSrcBucket     string
	debugFlag, logFlag bool
)

var allFlags = []cli.Flag{
	cli.BoolFlag{
		Name:  "insecure, i",
		Usage: "disable TLS certificate verification",
	},
	cli.BoolFlag{
		Name:  "log, l",
		Usage: "enable logging",
	},
	cli.BoolFlag{
		Name:  "debug",
		Usage: "enable debugging",
	},
	cli.StringFlag{
		Name:  "data-dir",
		Usage: "data directory",
	},
}

var subcommands = []cli.Command{
	migrateCmd,
	moveCmd,
	copyCmd,
	delCmd,
}

func mainAction(ctx *cli.Context) error {
	if !ctx.Args().Present() {
		cli.ShowCommandHelp(ctx, "")
		os.Exit(1)
	}
	command := ctx.Args().First()
	if command != "move" && command != "migrate" {
		cli.ShowCommandHelp(ctx, "")
		os.Exit(1)
	}
	debugFlag = ctx.Bool("debug")
	logFlag = ctx.Bool("log")
	fmt.Println(logFlag)
	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = os.Args[0]
	app.Author = "MinIO, Inc."
	app.Description = `Migration tool to move/copy objects to MinIO`
	app.Flags = []cli.Flag{}
	app.Action = mainAction
	app.Commands = subcommands
	app.Run(os.Args)
}
