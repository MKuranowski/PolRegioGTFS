#!/usr/bin/env sh
echo 'import { PolRegioGTFS } from "./src/parser.ts"; PolRegioGTFS.main();' |
deno run --unstable --allow-net --allow-read=gtfs --allow-write=gtfs --allow-run=zip -
