/*
 * Copyright (c) 2021, 2023 Mikołaj Kuranowski
 *
 * SPDX-License-Identifier: MIT
 */

import { SECOND } from "https://deno.land/std@0.207.0/datetime/mod.ts";
import * as xml from "https://deno.land/x/xmlp@v0.3.0/mod.ts";

// --- SCHEDULES JSON API --- //

export type TrainAttribute = [number, string, string, string, boolean, string];
export type Time = { hour: number; minute: number; second: number };

export interface Carrier {
    "id": number;
    "name": string;
    "slug": string;
    "update_date": string;
    "update_time": string;
}

export interface Brand {
    "id": number;
    "name": string;
    "carrier_id": number;
}

export interface CarrierTrain {
    "nr": number;
    "name": string | null;
    "brand": string;
}

export interface CarrierTrainsList {
    "id": number;
    "name": string;
    "trains": CarrierTrain[];
}

export interface TrainCalendar {
    "id": number;
    "train_nr": number;
    "train_number": string | null;
    "trainBrand": number;
    "dates": string[];
    "train_ids": number[];
    "date_train_map": Record<string, number>;
}

export interface TrainMetadata {
    "id": number;
    "train_nr": number;
    "name": string | null;
    "train_full_name": string;
    "run_desc": string;
    "carrier_id": number;
    "brand_id": number;
    "train_attributes": TrainAttribute[];
}

export interface TrainStop {
    "id": number;
    "station_id": number;
    "station_name": string;
    "station_slug": string;
    "station_ibnr": number;
    "train_id": number;
    "distance": number;
    "arrival": Time;
    "departure": Time;
    "position": number;
    "brand_id": number;
    "platform": string;
    "track": number | null;
    "entry_only": boolean;
    "exit_only": boolean;
}

export interface Train {
    "train": TrainMetadata;
    "stops": TrainStop[];
}

export interface Station {
    "id": number;
    "ibnr"?: number;
    "name": string;
    "name_slug": string;
}

export interface StationDepartureTarget {
    "departure": string;
    "nr": number;
    "name": string | null;
    "brand_name": string;
    "carrier_slug": string;
}

export interface StationArrivalTarget {
    "arrival": string;
    "nr": number;
    "name": string | null;
    "brand_name": string;
    "carrier_slug": string;
}

export interface StationDetails<TargetEntry> {
    "id": number;
    "station_coordinates": { latitude: number; longitude: number };
    "date": string;
    "list": { name: string; slug: string; target: TargetEntry[] }[];
    "popular_stations": { name: string; slug: string; quantity: number }[];
    "first_train": string;
    "last_train": string;
    "trains_number": number;
}

export class ResponseNotOK extends Error {
    response: Response;

    constructor(response: Response) {
        super();
        this.response = response;
        this.message = `Non-OK HTTP response (${response.status} ${response.statusText})`;
    }
}

/**
 * Endpoint is a class responsible for communicating with PolRegio API.
 */
export class Endpoint {
    /** How long should we wait between API calls (milliseconds) */
    readonly pause: number;

    /** The base URL for the API */
    readonly base: URL;

    /** The timestamp when the previous call was made */
    private lastCall = 0;

    /**
     * Creates a new Endpoint.
     * @param pause minimal time between API calls (milliseconds)
     * @param base base URL of the API
     */
    constructor(pause?: number, base?: string) {
        this.pause = pause ?? SECOND / 12;
        this.base = new URL(base ?? "https://bilety.polregio.pl/pl/");
    }

    /**
     * Makes a call to the API
     * @param where URL to request
     * @returns the JSON data
     */
    private async do_call(where: URL): Promise<unknown> {
        // Timeout
        const now: number = Date.now();
        const delta = now - this.lastCall;
        if (delta < this.pause) {
            await new Promise((r) => setTimeout(r, this.pause - delta));
        }
        this.lastCall = now;

        // Make the request
        const response = await fetch(where);

        // Verify success
        if (!response.ok) throw new ResponseNotOK(response);

        // Parse response
        return await response.json();
    }

    /**
     * Makes a call to the API, attempting `tries` times
     * @param where URL to request
     * @returns the JSON data
     */
    private async do_call_with_retry(where: URL, tries: number): Promise<unknown> {
        console.assert(tries > 0);
        let err: unknown = undefined;
        for (let i = 0; i < tries; ++i) {
            try {
                return await this.do_call(where);
            } catch (e) {
                console.error("Fetch to", where, "failed on attempt", i + 1, ":", e);
                err = e;
            }
        }
        throw err;
    }

    /**
     * Makes a call to the API, trying `timeout` times
     * @param where URL to request
     * @param accessors On success, access provided elements before returning the JSON object
     * @returns the JSON data
     */
    private async call(where: URL, accessors?: string[], tries?: number): Promise<unknown> {
        let data = await this.do_call_with_retry(where, tries ?? 3);

        // Get accessors
        for (const accessor of accessors ?? []) {
            // @ts-ignore: yeah it's unknown by design
            data = data[accessor];
        }

        return data;
    }

    /**
     * Fetches and returns all Carriers in the API
     */
    async carriers(): Promise<Carrier[]> {
        return await this.call(new URL("carriers", this.base), [
            "carriers",
        ]) as Carrier[];
    }

    /**
     * Fetches and returns all Brands in the API
     */
    async brands(): Promise<Brand[]> {
        return await this.call(new URL("brands", this.base), ["brands"]) as Brand[];
    }

    /**
     * Fetches and returns all Stations in the API
     */
    async stations(): Promise<Station[]> {
        return await this.call(new URL("stations", this.base), [
            "stations",
        ]) as Station[];
    }

    /**
     * Fetches and returns details about a particular station, with a list of departures for today
     * @param slug slug of the station
     */
    async stationDepartures(
        stationSlug: string,
    ): Promise<StationDetails<StationDepartureTarget>> {
        const url = new URL(stationSlug, new URL("station_departures/", this.base));
        return await this.call(url, ["station_departure"]) as StationDetails<
            StationDepartureTarget
        >;
    }

    /**
     * Fetches and returns details about a particular station, with a list of arrivals for today
     * @param slug slug of the station
     */
    async stationArrivals(
        stationSlug: string,
    ): Promise<StationDetails<StationArrivalTarget>> {
        const url = new URL(stationSlug, new URL("station_arrivals/", this.base));
        return await this.call(url, ["station_arrival"]) as StationDetails<
            StationArrivalTarget
        >;
    }

    /**
     * Fetches and returns all trains of a carrier, grouped by brands
     * @param slug slug of the carrier
     */
    async trains(carrierSlug: string): Promise<CarrierTrainsList[]> {
        const url = new URL("carrier_trains_lists", this.base);
        url.searchParams.set("carrier", carrierSlug);
        return await this.call(url, [
            "carrier_trains_lists",
        ]) as CarrierTrainsList[];
    }

    /**
     * Lists all 'versions' of a train and corresponding dates to every train 'version'.
     * @param train meta-data about a train
     */
    async trainCalendars(train: CarrierTrain): Promise<TrainCalendar[]> {
        const url = new URL("train_calendars", this.base);
        url.searchParams.set("brand", train.brand);
        url.searchParams.set("nr", train.nr.toString());
        if (train.name) url.searchParams.set("name", train.name);

        return await this.call(url, ["train_calendars"]) as TrainCalendar[];
    }

    /**
     * Fetches and returns metadata and all stations of a particular train version
     * @param train_id id of the train version
     */
    async trainData(trainID: number): Promise<Train> {
        const url = new URL(trainID.toString(), new URL("trains/", this.base));
        return await this.call(url) as Train;
    }
}

// --- STATIONS XML API --- //

export interface StationWithLocation {
    lat: string;
    lon: string;
    id: string;
    ibnr: string;
    name: string;
}

export async function getStationsWithLocation(
    url = "https://raw.githubusercontent.com/MKuranowski/PLRailMap/master/plrailmap.osm",
): Promise<[StationWithLocation[], Map<number, number>]> {
    // Make the request and verify success
    const resp = await fetch(url);
    if (!resp.ok) throw new ResponseNotOK(resp);

    const parser = new xml.SAXParser();
    const stations: StationWithLocation[] = [];
    const idChanges: Map<number, number> = new Map();
    const tags: Map<string, string> = new Map();

    // Handlers for parser events
    parser.on("start_element", (e) => {
        // When a new OSM element in encountered - clear the tags
        // as new ones will be loaded
        if (["node", "way", "relation"].indexOf(e.localPart) >= 0) {
            tags.clear();
        }
    }).on("end_element", (e) => {
        // When an end of element is encountered
        // the action depends on the kind of element
        if (e.localPart === "tag") {
            // If a <tag> element was encountered -
            // save it to the `tags` map.
            let k: string | undefined;
            let v: string | undefined;
            for (const attr of e.attributes) {
                if (attr.localPart === "k") k = attr.value;
                else if (attr.localPart === "v") v = attr.value;
            }
            tags.set(k!, v!);
        } else if (e.localPart === "node") {
            // If a <node> element was encountered
            // check if it was a station element -
            // and save it to the `stations` list.

            // Ignore non-stations
            if (tags.get("railway") !== "station") return;

            // Get station position from element attributes
            let lat: string | undefined;
            let lon: string | undefined;
            for (const attr of e.attributes) {
                if (attr.localPart === "lat") lat = attr.value;
                else if (attr.localPart === "lon") lon = attr.value;
            }

            // Force ID change if PolRegio uses an invalid ID
            if (tags.has("ref:2")) {
                idChanges.set(
                    parseInt(tags.get("ref:2")!),
                    parseInt(tags.get("ref")!),
                );
            }

            // Append to the list
            stations.push({
                "lat": lat!,
                "lon": lon!,
                id: tags.get("ref")!,
                ibnr: tags.get("ref:ibnr") ?? "",
                name: tags.get("name")!,
            });
        }
    });

    // Parse the XML file
    await parser.parse(await resp.text());
    return [stations, idChanges];
}
