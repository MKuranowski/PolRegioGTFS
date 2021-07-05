import { Endpoint, get_stations_with_locations } from "./api.ts";
import type { CarrierTrain, Station, Time, TrainAttribute, TrainStop } from "./api.ts";
import { CSVFile } from "./csv.ts";
import * as data from "./data.ts";
import * as datetime from "https://deno.land/std@0.100.0/datetime/mod.ts";
import * as color from "https://deno.land/std@0.100.0/fmt/colors.ts";
import { emptyDir } from "https://deno.land/std@0.100.0/fs/mod.ts";

type TrainLeg = { attrs: Set<number>, stops: TrainStop[] };

const WithFile = (func: (f: CSVFile) => Promise<void>, fname: string) => async () => {
    const f = await CSVFile.open(fname);
    await func(f).finally(f.close.bind(f));
}

const to_two_digits = (n: number) => n.toFixed(0).padStart(2, "0");
const time_to_int = (t: Time) => 3600*t.hour + 60*t.minute + t.second;
const time_to_str = (t: Time) => `${to_two_digits(t.hour)}:${to_two_digits(t.minute)}:${to_two_digits(t.second)}`;

const leg_is_bus = (l: TrainLeg) => l.stops[0].platform === "BUS"
                                    || l.attrs.has(data.ATTRS.BUS)
                                    || l.attrs.has(data.ATTRS.REPLACEMENT_BUS);

function* enumerate<T>(x: Iterable<T>, start: number = 0): Generator<[number, T], void, void> {
    for (const e of x) yield [start++, e];
}

function reverse_date_train_map(o: { [index: string]: number }): Map<number, Date[]> {
    const m: Map<number, Date[]> = new Map();

    for (const [date_str, train_id] of Object.entries(o)) {
        const date = datetime.parse(date_str, "yyyy-MM-dd");
        const arr = m.get(train_id);
        if (arr !== undefined)
            arr.push(date)
        else
            m.set(train_id, [date]);
    }

    return m;
}

/**
 * Modifies the provided list of stations to avoid time travel
 */
function fix_times(stops: TrainStop[]): TrainStop[] {
    let prev_dep: Time = { hour: 0, minute: 0, second: 0 };

    for (let stop of stops) {
        // Correct arrival
        while (time_to_int(stop.arrival) < time_to_int(prev_dep))
            stop.arrival.hour += 24;

        // Correct departure
        while (time_to_int(stop.departure) < time_to_int(stop.arrival))
            stop.departure.hour += 24;

        prev_dep = stop.departure;
    }

    return stops;
}

/**
 * Tries to detect at which indexes the train should be split into separate legs
 */
function get_break_legs_at(trip_id: number, attrs: TrainAttribute[], stop_names: string[]): Set<number> {
    let s: Set<number> = new Set();
    const last_idx = stop_names.length - 1;

    for (const attr of attrs) {
        // Only busses can force a leg break
        if (attr[0] !== data.ATTRS.BUS && attr[0] !== data.ATTRS.REPLACEMENT_BUS) continue;

        // Check start of bus section
        let idx = stop_names.indexOf(attr[2]);
        if (idx !== stop_names.lastIndexOf(attr[2]))
            console.warn(color.yellow("Train "
                         + color.cyan(trip_id.toString())
                         + ` has attribute (${attr[0]}) that starts on `
                         + `an ambiguous station (${attr[2]})\n\n`));

        if (idx !== 0 && idx !== last_idx) s.add(idx);

        // Check end of bus section
        idx = stop_names.lastIndexOf(attr[3]);
        if (idx !== stop_names.indexOf(attr[3]))
            console.warn(color.yellow("Train "
                         + color.cyan(trip_id.toString())
                         + ` has attribute (${attr[0]}) that ends on `
                         + `an ambiguous station (${attr[3]})\n\n`));

        if (idx !== 0 && idx !== last_idx) s.add(idx);
    }

    return s;
}

/**
 * Splits a list of stations into `legs` - as some parts of a trip
 * may be completed by a bus, some by a train.
 * @param stops List of all stations
 * @param force_break_at Additional set of stop indexes where a leg break should occur
 * @returns An array of legs (list of all stations)
 */
function split_legs(trip_id: number, stops: TrainStop[], attrs: TrainAttribute[]): TrainStop[][] {
    let legs: TrainStop[][] = [];
    let leg_so_far: TrainStop[] = [];
    const break_at = get_break_legs_at(trip_id, attrs, stops.map(s => s.station_name));

    for (const [idx, stop] of enumerate(stops)) {
        if (break_at.has(idx)) {
            // Finish the current leg by appending this
            // stop without the departure_time
            let stop_arr_only: TrainStop = { ...stop };
            stop_arr_only.departure = stop_arr_only.arrival;
            stop_arr_only.platform = "";
            leg_so_far.push(stop_arr_only);
            legs.push(leg_so_far);

            // Start the next leg with
            // this stop without the arrival
            let stop_dep_only: TrainStop = { ...stop };
            stop_dep_only.arrival = stop_dep_only.departure;
            leg_so_far = [stop_dep_only];
        } else {
            leg_so_far.push(stop);
        }
    }

    if (leg_so_far.length > 1) legs.push(leg_so_far);
    return legs;
}

function assign_attributes_to_legs(trip_id: number, attrs: TrainAttribute[], stops: TrainStop[][]): TrainLeg[] {
    let legs: TrainLeg[] = stops.map(s => { return { attrs: new Set(), stops: s }});

    // Get a list of boundary stops - first stops of the legs
    // and the last stop of the last leg
    let leg_boundaries = stops.map(leg_stops => leg_stops[0].station_name);
    leg_boundaries.push(stops.slice(-1)[0].slice(-1)[0].station_name);

    // Iterate over every attribute
    for (const attr of attrs) {
        const start_leg = leg_boundaries.indexOf(attr[2]);
        const end_leg = leg_boundaries.lastIndexOf(attr[3]);

        if (start_leg < 0 || end_leg < 0) {
            // console.warn(`Train ${trip_id} has attribute that falls outside leg boundaries (${attr[0]})\n\n`);
            continue;
        }

        for (let i = start_leg; i < end_leg; ++i)
            legs[i].attrs.add(attr[0]);
    }

    return legs;
}

export class PolRegioGTFS {
    /** Latest update time, as returned by the carriers endpoint */
    update_time: Date = new Date(0);

    /** All known carriers (id → slug) */
    known_carriers: Map<number, string> = new Map();

    /** Set of IDs of all used stations (id -> Station) */
    used_stations: Map<string, Station> = new Map();

    /** Set of brand IDs for which a bus routes needs to be generated */
    brands_with_busses: Set<number> = new Set();

    /** The Endpoint instance to request data */
    api: Endpoint = new Endpoint();

    trips?: CSVFile
    times?: CSVFile
    dates?: CSVFile
    transfers?: CSVFile

    /**
     * Prepares attached file handles, needs to be called prior
     * to starting all of the parsing.
     */
    async open(): Promise<void> {
        // Open files
        await emptyDir("gtfs");
        [this.trips, this.times, this.dates, this.transfers] = await Promise.all(
            ["gtfs/trips.txt", "gtfs/stop_times.txt", "gtfs/calendar_dates.txt", "gtfs/transfers.txt"]
            .map(f => CSVFile.open(f))
        )

        // Write headers
        this.trips.write_row(["route_id", "service_id", "trip_id", "trip_short_name",
                              "trip_headsign", "wheelchair_accessible", "bikes_allowed"]);

        this.times.write_row(["trip_id", "stop_sequence", "stop_id", "arrival_time",
                              "departure_time", "platform"]);

        this.dates.write_row(["service_id", "date", "exception_type"]);

        this.transfers.write_row(["from_stop_id", "to_stop_id", "from_trip_id", "to_trip_id",
                                  "transfer_type"]);
    }

    /**
     * Closes opened file handles
     */
    close(): void {
        this.trips?.close();
        this.times?.close();
        this.dates?.close();
        this.transfers?.close();
    }

    /**
     * Parses agency data from the API and writes them to the provided CSVFile.
     * Also sets the `update_time` attribute.
     */
    private async parse_agencies_into(f: CSVFile): Promise<void> {
        // Write headers
        await f.write_row([
            "agency_id", "agency_name", "agency_url", "agency_lang",
            "agency_timezone", "agency_phone"
        ]);

        for (const agency of await this.api.carriers()) {
            // Get data for agency
            const agency_data = data.AGENCIES.get(agency.id);
            if (agency_data === undefined)
                throw `Agency data for carrier ${agency.id} (${agency.name}) is missing`;

            // Write to agency.txt
            await f.write_row([
                agency.id,
                agency_data.name,
                agency_data.url,
                "pl",
                "Europe/Warsaw",
                agency_data.phone,
            ]);

            this.known_carriers.set(agency.id, agency.slug);

            // Update Time
            let update_time = datetime.parse(`${agency.update_time} ${agency.update_date}`,
                                             "hh:mm dd.MM.yyyy");
            if (update_time > this.update_time) this.update_time = update_time;
        }
    }

    /**
     * Writes agency.txt based on data from the API;
     * while also setting the `update_time` attribute.
     */
    parse_agencies = WithFile(this.parse_agencies_into.bind(this), "gtfs/agency.txt");

    /**
     * Parses route data from the API and writes them to the provided CSVFile.
     * Needs to be called _after_ parsing trains - as this method expects
     * the `brands_with_buses` Set to be filled.
     */
    private async parse_routes_into(f: CSVFile): Promise<void> {
        // Write header
        await f.write_row([
            "agency_id", "route_id", "route_short_name", "route_long_name",
            "route_type", "route_color", "route_text_color"
        ]);

        // Iterate over every brand - which is mapped to GTFS routes
        for (const route of await this.api.brands()) {
            // Get external data
            const route_data = data.ROUTES.get(route.id);
            if (route_data === undefined)
                throw `Agency data for carrier ${route.id} (${route.name}) is missing`;

            // Extract colors
            let [color, text_color] = route_data.color ?? data.DEFAULT_COLOR;

            // Write to routes.txt
            await f.write_row([
                route.carrier_id,
                route.id,
                route_data.code,
                route_data.name,
                "2",
                color,
                text_color,
            ]);

            // Check if a bus route needs to be added
            if (this.brands_with_busses.has(route.id)) {
                [color, text_color] = route_data.bus_color ?? data.BUS_COLOR;
                await f.write_row([
                    route.carrier_id,
                    route.id.toString() + "-BUS",
                    data.BUS_CODE_PREFIX + route_data.code,
                    route_data.name + data.BUS_NAME_SUFFIX,
                    "3",
                    color,
                    text_color,
                ]);
            }
        }

    }

    /**
     * Writes routes.txt based on data from the API.
     * Needs to be called _after_ parsing trains - as this method expects
     * the `brands_with_buses` Set to be filled.
     */
    parse_routes = WithFile(this.parse_routes_into.bind(this), "gtfs/routes.txt");

    /**
     * Parses and writes all versions of a particular train
     * trips.txt, stop_times.txt, calendar_dates.txt and transfers.txt will be modified.
     */
    async parse_train(train: CarrierTrain): Promise<void> {
        console.log(`\x1B[2A\x1B[KParsing trains: ${train.brand} ${train.nr} '${train.name ?? ''}'\n`);
        const data = await this.api.train_calendars(train);

        // Ensure only one calendar exists
        if (data.length !== 1)
            throw `Train ${train.brand} ${train.nr} '${train.name ?? ''}' has multiple/zero calendars`;

        const calendar = data[0];
        const date_map = reverse_date_train_map(calendar.date_train_map);

        for (const [trip_id, dates] of date_map.entries()) {
            await this.parse_trip(trip_id);

            for (const date of dates)
                await this.dates?.write_row([
                    trip_id,
                    datetime.format(date, "yyyyMMdd"),
                    "1",
                ]);
        }
    }

    /**
     * Parses and writes a particular version of a train.
     * trips.txt, stop_times.txt and transfers.txt will be modified.
     */
    async parse_trip(trip_id: number): Promise<void> {
        console.log(`\x1B[1A\x1B[KParsing train version: ${trip_id}`);
        const data = await this.api.train_data(trip_id);

        fix_times(data.stops);
        const leg_stops = split_legs(trip_id, data.stops, data.train.train_attributes)
        const legs = assign_attributes_to_legs(trip_id, data.train.train_attributes,
                                               leg_stops);

        let gtfs_trip: (string | number)[] = [
            data.train.brand_id,
            trip_id,
            trip_id,
            data.train.train_full_name,
            data.stops[data.stops.length - 1].station_name,
            "2",
            "2",
        ]

        if (legs.length > 1)
            await this.write_train_many_legs(gtfs_trip, legs);
        else if (legs.length === 1)
            await this.write_train_single_leg(gtfs_trip, legs[0]);
        else
            throw `Train ${trip_id} has no legs`;
    }

    private async write_train_single_leg(gtfs_trip: (string | number)[], leg: TrainLeg): Promise<void> {
        // Check for replacement busses
        if (leg_is_bus(leg)) {
            this.brands_with_busses.add(gtfs_trip[0] as number);
            gtfs_trip[0] = gtfs_trip[0].toString() + "-BUS";
        }

        // Write to trips.txt
        await this.trips?.write_row(gtfs_trip);

        // Write to stop_times.txt
        for (const [seq, stop] of enumerate(leg.stops)) {
            this.used_stations.set(
                stop.station_id.toString(),
                { id: stop.station_id, ibnr: stop.station_ibnr,
                  name: stop.station_name, name_slug: stop.station_slug }
            );

            await this.times?.write_row([
                gtfs_trip[2],
                seq,
                stop.station_id,
                time_to_str(stop.arrival),
                time_to_str(stop.departure),
                stop.platform,
            ]);
        }
    }

    private async write_train_many_legs(base_gtfs_trip: (string | number)[], legs: TrainLeg[]) {
        let previous_leg_id: string = "";

        for (const [suffix, leg] of enumerate(legs)) {
            const leg_id = `${base_gtfs_trip[2]}-${suffix}`;

            // Copy trips.txt entry and update trip_id
            const leg_gtfs_trip = base_gtfs_trip.slice();
            leg_gtfs_trip[2] = leg_id;

            // Write to trips.txt and stop_times.txt (and also handle bus legs)
            await this.write_train_single_leg(leg_gtfs_trip, leg);

            // Write to transfers.txt
            if (previous_leg_id)
                await this.transfers?.write_row([
                    leg.stops[0].station_id,
                    leg.stops[0].station_id,
                    previous_leg_id,
                    leg_id,
                    "1",
                ]);

            previous_leg_id = leg_id;
        }
    }

    async parse_all_trains(): Promise<void> {
        for (const [, agency_slug] of this.known_carriers)
            for (const brand_trains of await this.api.trains(agency_slug))
                for (const train of brand_trains.trains)
                    await this.parse_train(train);
    }

    /**
     * Parses stops from an external source (github.com/MKuranowski/PLRailMap)
     * and saves **used** stops
     */
    private async parse_stops_info(f: CSVFile): Promise<void> {
        await f.write_row(["stop_id", "stop_name", "stop_lat", "stop_lon", "stop_IBNR"]);

        for (const s of await get_stations_with_locations()) {
            if (!this.used_stations.delete(s.id)) continue;
            await f.write_row([s.id, s.name, s.lat, s.lon, s.ibnr]);
        }

        if (this.used_stations.size > 0) {
            console.log(color.red("Missing stations"))
            console.table(Array.from(this.used_stations.values()), ["id", "name", "ibnr"]);
        }
    }

    parse_stops = WithFile(this.parse_stops_info.bind(this), "gtfs/stops.txt");

    async parse_all(): Promise<void> {
        console.log("Parsing agencies");
        await this.parse_agencies();

        if (this.known_carriers.size === 0)
            throw "No carriers returned from the API";

        console.log("Parsing trains: warming up\n");
        await this.parse_all_trains();

        console.log("\x1B[2A\x1B[KParsing trains: done\n\x1B[KParsing stops");
        await this.parse_stops();

        console.log("Parsing routes");
        await this.parse_routes();
    }

    static async main() {
        const parser = new PolRegioGTFS();
        await parser.open();
        await parser.parse_all().finally(parser.close.bind(this));
    }
}
