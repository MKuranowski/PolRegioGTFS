/*
 * Copyright (c) 2021 Mikołaj Kuranowski
 *
 * SPDX-License-Identifier: MIT
 */

import { Endpoint, getStationsWithLocation } from "./api.ts";
import type {
  CarrierTrain,
  Station,
  Time,
  TrainAttribute,
  TrainStop,
} from "./api.ts";
import { CSVFile } from "./csv.ts";
import * as data from "./data.ts";
import * as datetime from "https://deno.land/std@0.100.0/datetime/mod.ts";
import * as color from "https://deno.land/std@0.100.0/fmt/colors.ts";
import { emptyDir } from "https://deno.land/std@0.100.0/fs/mod.ts";

// TrainLeg is a type used internally for representing TrainLegs with assigned attributes
type TrainLeg = { attrs: Set<number>; stops: TrainStop[] };

/**
 * WithFile wraps an async function expecting a CSVFile to
 * automatically open and close CSVFile with a provided filename
 * @param func the async function expecting CSVFile as an argument
 * @param fname filename of the target CSVFile
 * @returns wrapped function
 */
function WithFile<T>(
  func: (f: CSVFile) => Promise<T>,
  fname: string,
): () => Promise<T> {
  return async () => {
    const f = await CSVFile.open(fname);
    return await func(f).finally(f.close.bind(f));
  };
}

/** Converts an integer to a two-letter string representation */
const toTwoDigits = (n: number) => n.toFixed(0).padStart(2, "0");

/** Converts a Time object to an int - representing an amount of seconds */
const timeToInt = (t: Time) => 3600 * t.hour + 60 * t.minute + t.second;

/** Converts a Time object to a nice string representation */
const timeToStr = (t: Time) =>
  [t.hour, t.minute, t.second].map(toTwoDigits).join(":");

/** Checks if a TrainLeg is actually operated by a bus */
const legIsBus = (l: TrainLeg) =>
  l.stops[0].platform === "BUS" ||
  l.attrs.has(data.ATTRS.BUS) ||
  l.attrs.has(data.ATTRS.REPLACEMENT_BUS);

/**
 * Implementation of Python's enumerate.
 * Yields elements from an iterable and their associated indexes.
 * @param iterable The iterable from which elements ought to be returned
 * @param start To very first index, defaults to 0
 */
function* enumerate<T>(
  iterable: Iterable<T>,
  start = 0,
): Generator<[number, T], void, void> {
  for (const elem of iterable) yield [start++, elem];
}

/**
 * Reverses APIs "date_train_map"
 * @param o "date_train_map" object (mapping date -> tripID)
 * @returns mapping tripID -> date[]
 */
function reverseDateTrainMap(o: Record<string, number>): Map<number, Date[]> {
  const m: Map<number, Date[]> = new Map();

  for (const [dateStr, tripID] of Object.entries(o)) {
    const date = datetime.parse(dateStr, "yyyy-MM-dd");
    const arr = m.get(tripID);
    if (arr !== undefined) {
      arr.push(date);
    } else {
      m.set(tripID, [date]);
    }
  }

  return m;
}

/**
 * Modifies the provided list of stations to avoid time travel
 */
function fixTimes(stops: TrainStop[]): TrainStop[] {
  let prevDep: Time = { hour: 0, minute: 0, second: 0 };

  for (const stop of stops) {
    // Correct arrival
    while (timeToInt(stop.arrival) < timeToInt(prevDep)) {
      stop.arrival.hour += 24;
    }

    // Correct departure
    while (timeToInt(stop.departure) < timeToInt(stop.arrival)) {
      stop.departure.hour += 24;
    }

    prevDep = stop.departure;
  }

  return stops;
}

/**
 * Tries to detect at which indexes the train should be split into separate legs
 */
function getBreakLegsAt(
  tripID: number,
  attrs: TrainAttribute[],
  stopNames: string[],
): Set<number> {
  const s: Set<number> = new Set();
  const lastIDX = stopNames.length - 1;

  for (const attr of attrs) {
    // Only busses can force a leg break
    if (attr[0] !== data.ATTRS.BUS && attr[0] !== data.ATTRS.REPLACEMENT_BUS) {
      continue;
    }

    // Check start of bus section
    let idx = stopNames.indexOf(attr[2]);
    if (idx !== stopNames.lastIndexOf(attr[2])) {
      console.warn(color.yellow(
        "Train " +
          color.cyan(tripID.toString()) +
          ` has attribute (${attr[0]}) that starts on ` +
          `an ambiguous station (${attr[2]})\n\n`,
      ));
    }

    if (idx !== 0 && idx !== lastIDX) s.add(idx);

    // Check end of bus section
    idx = stopNames.lastIndexOf(attr[3]);
    if (idx !== stopNames.indexOf(attr[3])) {
      console.warn(color.yellow(
        "Train " +
          color.cyan(tripID.toString()) +
          ` has attribute (${attr[0]}) that ends on ` +
          `an ambiguous station (${attr[3]})\n\n`,
      ));
    }

    if (idx !== 0 && idx !== lastIDX) s.add(idx);
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
function splitLegs(
  tripID: number,
  stops: TrainStop[],
  attrs: TrainAttribute[],
): TrainStop[][] {
  const legs: TrainStop[][] = [];
  let legSoFar: TrainStop[] = [];
  const breakAt = getBreakLegsAt(
    tripID,
    attrs,
    stops.map((s) => s.station_name),
  );

  for (const [idx, stop] of enumerate(stops)) {
    if (breakAt.has(idx)) {
      // Finish the current leg by appending this
      // stop without the departure_time
      const stopArrOnly: TrainStop = { ...stop };
      stopArrOnly.departure = stopArrOnly.arrival;
      stopArrOnly.platform = "";
      legSoFar.push(stopArrOnly);
      legs.push(legSoFar);

      // Start the next leg with
      // this stop without the arrival
      const stopDepOnly: TrainStop = { ...stop };
      stopDepOnly.arrival = stopDepOnly.departure;
      legSoFar = [stopDepOnly];
    } else {
      legSoFar.push(stop);
    }
  }

  if (legSoFar.length > 1) legs.push(legSoFar);
  return legs;
}

/**
 * Assigns attributes to all of the trip legs
 */
function assignAttrsToLegs(
  _tripID: number,
  attrs: TrainAttribute[],
  stops: TrainStop[][],
): TrainLeg[] {
  const legs: TrainLeg[] = stops.map((s) => {
    return { attrs: new Set(), stops: s };
  });

  // Get a list of boundary stops - first stops of the legs
  // and the last stop of the last leg
  const legBoundaries = stops.map((leg_stops) => leg_stops[0].station_name);
  legBoundaries.push(stops.slice(-1)[0].slice(-1)[0].station_name);

  // Iterate over every attribute
  for (const attr of attrs) {
    // Get indexes of the (first) leg matching the attribute start station
    // and (last) leg starting with the attribute end station.
    const startLeg = legBoundaries.indexOf(attr[2]);
    const endLeg = legBoundaries.lastIndexOf(attr[3]);

    // Ensure stations fall on leg boundaries
    if (startLeg < 0 || endLeg < 0) {
      // console.warn(`Train ${_tripID} has attribute that falls outside leg boundaries (${attr[0]})\n\n`);
      continue;
    }

    // Distribute the attribute code to matching legs
    for (let i = startLeg; i < endLeg; ++i) {
      legs[i].attrs.add(attr[0]);
    }
  }

  return legs;
}

/**
 * PolRegioGTFS is the main class responsible for parsing the data
 */
export class PolRegioGTFS {
  /** Latest update time, as returned by the carriers endpoint */
  updateTime: Date = new Date(0);

  /** All known carriers (id → slug) */
  knownCarriers: Map<number, string> = new Map();

  /** IDs of all used stations (id -> Station) */
  usedStations: Map<string, Station> = new Map();

  /** Brand IDs for which a bus routes needs to be generated */
  brandsWithBusses: Set<number> = new Set();

  /** The Endpoint instance to request data */
  api: Endpoint = new Endpoint();

  // CSVFile handles for some of the files used by the Parser
  trips?: CSVFile;
  times?: CSVFile;
  dates?: CSVFile;
  transfers?: CSVFile;

  /**
   * Prepares attached file handles, needs to be called prior
   * to starting all of the parsing.
   */
  async open(): Promise<void> {
    // Open files
    await emptyDir("gtfs");
    [this.trips, this.times, this.dates, this.transfers] = await Promise.all(
      [
        "gtfs/trips.txt",
        "gtfs/stop_times.txt",
        "gtfs/calendar_dates.txt",
        "gtfs/transfers.txt",
      ]
        .map((f) => CSVFile.open(f)),
    );

    // Write headers
    await Promise.all([
      this.trips.write_row([
        "route_id",
        "service_id",
        "trip_id",
        "trip_short_name",
        "trip_headsign",
        "wheelchair_accessible",
        "bikes_allowed",
      ]),
      this.times.write_row([
        "trip_id",
        "stop_sequence",
        "stop_id",
        "arrival_time",
        "departure_time",
        "platform",
        "official_dist_traveled",
      ]),
      this.dates.write_row(["service_id", "date", "exception_type"]),
      this.transfers.write_row([
        "from_stop_id",
        "to_stop_id",
        "from_trip_id",
        "to_trip_id",
        "transfer_type",
      ]),
    ]);
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
   * Also sets the `update_time` and `known_carriers` attributes.
   */
  private async parseAgenciesInto(f: CSVFile): Promise<void> {
    // Write headers
    await f.write_row([
      "agency_id",
      "agency_name",
      "agency_url",
      "agency_lang",
      "agency_timezone",
      "agency_phone",
    ]);

    for (const agency of await this.api.carriers()) {
      // Get data for agency
      const meta = data.AGENCIES.get(agency.id);
      if (meta === undefined) {
        throw `Agency data for carrier ${agency.id} (${agency.name}) is missing`;
      }

      // Write to agency.txt
      await f.write_row([
        agency.id,
        meta.name,
        meta.url,
        "pl",
        "Europe/Warsaw",
        meta.phone,
      ]);

      this.knownCarriers.set(agency.id, agency.slug);

      // Update Time
      const updateTime = datetime.parse(
        `${agency.update_time} ${agency.update_date}`,
        "HH:mm dd.MM.yyyy",
      );
      if (updateTime > this.updateTime) this.updateTime = updateTime;
    }
  }

  /**
   * Writes agency.txt based on data from the API;
   * while also setting the `update_time` attribute.
   */
  parseAgencies = WithFile(
    this.parseAgenciesInto.bind(this),
    "gtfs/agency.txt",
  );

  /**
   * Parses route data from the API and writes them to the provided CSVFile.
   * Needs to be called _after_ parsing trains - as this method expects
   * `brandsWithBusses` to be filled.
   */
  private async parseRoutesInto(f: CSVFile): Promise<void> {
    // Write header
    await f.write_row([
      "agency_id",
      "route_id",
      "route_short_name",
      "route_long_name",
      "route_type",
      "route_color",
      "route_text_color",
    ]);

    // Iterate over every brand - which is mapped to GTFS routes
    for (const route of await this.api.brands()) {
      // Get external data
      const meta = data.ROUTES.get(route.id);
      if (meta === undefined) {
        throw `Agency data for carrier ${route.id} (${route.name}) is missing`;
      }

      // Extract colors
      let [color, textColor] = meta.color ?? data.DEFAULT_COLOR;

      // Write to routes.txt
      await f.write_row([
        route.carrier_id,
        route.id,
        meta.code,
        meta.name,
        "2",
        color,
        textColor,
      ]);

      // Check if a bus route needs to be added
      if (this.brandsWithBusses.has(route.id)) {
        [color, textColor] = meta.busColor ?? data.BUS_COLOR;
        await f.write_row([
          route.carrier_id,
          route.id.toString() + "-BUS",
          data.BUS_CODE_PREFIX + meta.code,
          meta.name + data.BUS_NAME_SUFFIX,
          "3",
          color,
          textColor,
        ]);
      }
    }
  }

  /**
   * Writes routes.txt based on data from the API.
   * Needs to be called _after_ parsing trains - as this method expects
   * the `brands_with_buses` Set to be filled.
   */
  parseRoutes = WithFile(this.parseRoutesInto.bind(this), "gtfs/routes.txt");

  /**
   * Parses and writes all versions of a particular train
   * trips.txt, stop_times.txt, calendar_dates.txt and transfers.txt will be modified.
   */
  async parseTrain(train: CarrierTrain): Promise<void> {
    console.log(
      `\x1B[2A\x1B[KParsing trains: ${train.brand} ${train.nr} '${train.name ??
        ""}'\n`,
    );
    const data = await this.api.trainCalendars(train);

    // Ensure only one calendar exists
    if (data.length !== 1) {
      throw `Train ${train.brand} ${train.nr} '${train.name ??
        ""}' has multiple/zero calendars`;
    }

    const calendar = data[0];
    const dateMap = reverseDateTrainMap(calendar.date_train_map);

    for (const [tripID, dates] of dateMap.entries()) {
      const ok = await this.parseTrip(tripID);
      if (!ok) continue;

      for (const date of dates) {
        await this.dates?.write_row([
          tripID,
          datetime.format(date, "yyyyMMdd"),
          "1",
        ]);
      }
    }
  }

  /**
   * Parses and writes a particular version of a train.
   * trips.txt, stop_times.txt and transfers.txt will be modified.
   */
  async parseTrip(tripID: number): Promise<boolean> {
    console.log(`\x1B[1A\x1B[KParsing train version: ${tripID}`);
    const data = await this.api.trainData(tripID);

    // Ensure this train has stops
    if (data.stops.length === 0) {
      console.warn(color.yellow(
        "Train " + color.cyan(tripID.toString()) + " has no stops!",
      ));
      return false;
    }

    fixTimes(data.stops);
    const legStops = splitLegs(tripID, data.stops, data.train.train_attributes);
    const legs = assignAttrsToLegs(
      tripID,
      data.train.train_attributes,
      legStops,
    );

    const gtfsTrip: (string | number)[] = [
      data.train.brand_id,
      tripID,
      tripID,
      data.train.train_full_name,
      data.stops[data.stops.length - 1].station_name,
      "2",
      "2",
    ];

    if (legs.length > 1) {
      await this.writeMultipleLegs(gtfsTrip, legs);
    } else if (legs.length === 1) {
      await this.writeSingleLeg(gtfsTrip, legs[0]);
    } else {
      throw `Train ${tripID} has no legs`;
    }

    return true;
  }

  /**
   * Writes a single leg to stop_times.txt.
   * Also checks (and modifies) route_id column if this leg is operated by a bus
   * Might update `brandsWithBusses`.
   */
  private async writeSingleLeg(
    gtfsTrip: (string | number)[],
    leg: TrainLeg,
  ): Promise<void> {
    // Check for replacement busses
    if (legIsBus(leg)) {
      this.brandsWithBusses.add(gtfsTrip[0] as number);
      gtfsTrip[0] = gtfsTrip[0].toString() + "-BUS";
    }

    // Get distance offset
    const distOffset = leg.stops[0].distance;

    // Write to trips.txt
    await this.trips?.write_row(gtfsTrip);

    // Write to stop_times.txt
    for (const [seq, stop] of enumerate(leg.stops)) {
      this.usedStations.set(
        stop.station_id.toString(),
        {
          id: stop.station_id,
          ibnr: stop.station_ibnr,
          name: stop.station_name,
          name_slug: stop.station_slug,
        },
      );

      await this.times?.write_row([
        gtfsTrip[2],
        seq,
        stop.station_id,
        timeToStr(stop.arrival),
        timeToStr(stop.departure),
        stop.platform,
        (stop.distance - distOffset).toFixed(),
      ]);
    }
  }

  /**
   * Writes multiple legs to trips.txt, stop_times.txt and transfers.txt.
   * The baseGtfsTrip will be used as a template for all trips.txt rows
   * - trip_id will be modified with a suffix representing a leg,
   * - route_id might be modified if a leg is operated by a bus.
   */
  private async writeMultipleLegs(
    baseGtfsTrip: (string | number)[],
    legs: TrainLeg[],
  ) {
    let previousLegID = "";

    for (const [suffix, leg] of enumerate(legs)) {
      const legID = `${baseGtfsTrip[2]}-${suffix}`;

      // Copy trips.txt entry and update trip_id
      const legGtfsTrip = baseGtfsTrip.slice();
      legGtfsTrip[2] = legID;

      // Write to trips.txt and stop_times.txt (and also handle bus legs)
      await this.writeSingleLeg(legGtfsTrip, leg);

      // Write to transfers.txt
      if (previousLegID) {
        await this.transfers?.write_row([
          leg.stops[0].station_id,
          leg.stops[0].station_id,
          previousLegID,
          legID,
          "1",
        ]);
      }

      previousLegID = legID;
    }
  }

  /**
   * Walks over every train of every known carrier (from `knownCarriers`) and
   * calls parseTrain on it.
   */
  async parseAllTrains(): Promise<void> {
    for (const [, agencySlug] of this.knownCarriers) {
      for (const brand of await this.api.trains(agencySlug)) {
        for (const train of brand.trains) {
          await this.parseTrain(train);
        }
      }
    }
  }

  /**
   * Parses stops from an external source (github.com/MKuranowski/PLRailMap)
   * and saves **used** stops
   */
  private async parseStopsInto(f: CSVFile): Promise<void> {
    const unknownStations = new Map(this.usedStations);
    await f.write_row([
      "stop_id",
      "stop_name",
      "stop_lat",
      "stop_lon",
      "stop_IBNR",
    ]);

    for (const s of await getStationsWithLocation()) {
      // Try to remove this station from unknownStations
      // - if the removal failed - this station was not used
      // and can be ignored
      if (!unknownStations.delete(s.id)) continue;
      await f.write_row([s.id, s.name, s.lat, s.lon, s.ibnr]);
    }

    if (unknownStations.size > 0) {
      console.log(color.red("Missing stations"));
      console.table(Array.from(unknownStations.values()), [
        "id",
        "name",
        "ibnr",
      ]);
      throw `${unknownStations.size} unknown stations were used by the API`;
    }
  }

  /**
   * Writes stops.txt based on data from an external source (github.com/MKuranowski/PLRailMap)
   * Needs to be called _after_ parsing trains - as this method expects
   * `usedStations` to be filled.
   */
  parseStops = WithFile(this.parseStopsInto.bind(this), "gtfs/stops.txt");

  async parseAll(): Promise<void> {
    console.log("Parsing agencies");
    await this.parseAgencies();

    if (this.knownCarriers.size === 0) {
      throw "No carriers returned from the API";
    }

    console.log("Parsing trains: warming up\n");
    await this.parseAllTrains();

    console.log("\x1B[2A\x1B[KParsing trains: done\n\x1B[KParsing stops");
    await this.parseStops();

    console.log("Parsing routes");
    await this.parseRoutes();

    console.log("Compressing to polregio.zip");
    await this.compress();
  }

  async compress(): Promise<void> {
    const p = Deno.run({ cmd: ["zip", "-rj", "polregio.zip", "gtfs"] });
    const s = await p.status();
    if (!s.success) throw "Unable to compress files into polregio.zip";
  }

  static async main() {
    const parser = new PolRegioGTFS();
    await parser.open();
    await parser.parseAll().finally(parser.close.bind(this));
  }
}
