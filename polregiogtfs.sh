#!/usr/bin/env sh
# Copyright (c) 2021, 2024 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT
echo 'import { PolRegioGTFS } from "./src/parser.ts"; await PolRegioGTFS.main();' |
deno run --allow-net --allow-read=gtfs --allow-write=gtfs --allow-run=zip -
