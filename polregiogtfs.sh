#!/usr/bin/env sh
# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT
echo 'import { PolRegioGTFS } from "./src/parser.ts"; PolRegioGTFS.main();' |
deno run --unstable --allow-net --allow-read=gtfs --allow-write=gtfs --allow-run=zip -
