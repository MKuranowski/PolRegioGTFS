type Color = [string, string];
type Route = {
    code: string;
    name: string;
    color?: Color;
    bus_color?: Color;
};
type Agency = {
    name: string;
    url: string;
    phone?: string;
};

export const DEFAULT_COLOR: Color = ["E50000", "FFFFFF"];
export const BUS_COLOR: Color = ["F78F1E", "000000"];
export const BUS_CODE_PREFIX: string = "ZKA ";
export const BUS_NAME_SUFFIX: string = " - Zastępcza Komunikacja Autobusowa";

export const ATTRS = {
    BIKES: 9,
    WHEELCHAIR: 10,
    BUS: 56,
    REPLACEMENT_BUS: 100,
};

export const ROUTES: Map<number, Route> = new Map([
    [3, { code: "REG", name: "Regio" }],
    [4, { code: "IR", name: "interRegio" }],
    [20, { code: "PRS", name: "PolRegio - Specjalny" }],
    [48, { code: "SR", name: "superRegio" }],
]);

export const AGENCIES: Map<number, Agency> = new Map([
    [4, { name: "PolRegio", url: "https://polregio.pl/", phone: "+48703202020" }],
]);
