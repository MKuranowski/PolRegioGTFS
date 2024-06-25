/*
 * Copyright (c) 2021, 2024 Mikołaj Kuranowski
 *
 * SPDX-License-Identifier: MIT
 */

type Color = [string, string];
type Route = {
    code: string;
    name: string;
    color?: Color;
    busColor?: Color;
};
type Agency = {
    name: string;
    url: string;
    phone?: string;
};

export const DEFAULT_COLOR: Color = ["E50000", "FFFFFF"];
export const BUS_COLOR: Color = ["F78F1E", "000000"];
export const BUS_CODE_PREFIX = "ZKA ";
export const BUS_NAME_SUFFIX = " - Zastępcza Komunikacja Autobusowa";

export const ATTRS = {
    BIKES: 9,
    WHEELCHAIR: 10,
    BIKES_ALLOWED: 50,
    BUS: 56,
    REPLACEMENT_BUS: 100,
};

export const ROUTES: Map<number, Route> = new Map([
    [3, { code: "REG", name: "Regio" }],
    [4, { code: "IR", name: "interRegio" }],
    [18, { code: "MR", name: "musicRegio" }],
    [20, { code: "PRS", name: "PolRegio - Specjalny" }],
    [48, { code: "SR", name: "superRegio" }],
]);

export const AGENCIES: Map<number, Agency> = new Map([
    [4, { name: "PolRegio", url: "https://polregio.pl/", phone: "+48703202020" }],
]);
