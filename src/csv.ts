/*
 * Copyright (c) 2021 Mikołaj Kuranowski
 *
 * SPDX-License-Identifier: MIT
 */

/** Any type that can be serialized to a string */
type Stringable = string | null | undefined | { toString(): string };

/** Regular expression testing whether a string needs to be escaped */
const needsEscape = /"|,|\r|\n/;

/** Converts a `Stringable` object to a string */
const stringify = (o: Stringable) => o === null || o === undefined ? "" : o.toString();

/** Escapes a string for exporting in CSV */
const escapeCell = (cell: string) =>
    needsEscape.test(cell) ? `"${cell.replaceAll('"', '""')}"` : cell;

/** Serializes multiple objects to a CSV row, terminated with \r\n */
export const toCSV = (row: Stringable[]) =>
    row.map((e) => escapeCell(stringify(e))).join(",") + "\r\n";

/**
 * CSVFile is a small abstraction over Deno.FsFile to help with writing CSV files.
 */
export class CSVFile {
    handle: Deno.FsFile;
    private encoder = new TextEncoder();

    /** Create a CSVFile on a particular file */
    constructor(handle: Deno.FsFile) {
        this.handle = handle;
    }

    /**
     * Opens a CSV file for writing - creating a file if it does not exist,
     * and truncating one if it were to exist.
     * @param filename name of the file to open
     */
    static async open(filename: string | URL): Promise<CSVFile> {
        return new CSVFile(
            await Deno.open(filename, { write: true, truncate: true, create: true }),
        );
    }

    /** Write a single row to the file */
    async write_row(row: Stringable[]): Promise<void> {
        const view = this.encoder.encode(toCSV(row));
        await this.handle.write(view);
    }

    /** Close the underlying file */
    close() {
        this.handle.close();
    }
}
