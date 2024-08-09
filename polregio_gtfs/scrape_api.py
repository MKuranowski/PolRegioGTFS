import logging
from collections import defaultdict
from typing import Iterable, Mapping

import impuls
from impuls.model import Date, TimePoint

from . import api

BUS_ATTRIBUTE_IDS = {2, 56, 100}


class ScrapeAPI(impuls.Task):
    def __init__(self) -> None:
        super().__init__()
        self.endpoint = api.Endpoint()
        self._added_stations = set[int]()

    def clear(self) -> None:
        self._added_stations.clear()

    def execute(self, r: impuls.TaskRuntime) -> None:
        self.clear()
        self._check_carriers()
        with r.db.transaction():
            self._scrape_carrier("polregio-przewozy-regionalne", r.db)

    def _check_carriers(self) -> None:
        carriers = {i["slug"] for i in self.endpoint.carriers()}
        if "polregio-przewozy-regionalne" not in carriers:
            raise impuls.errors.DataError(
                "API is not reporting that carrier 'polregio-przewozy-regionalne' is available. "
                f"All carriers: {carriers}."
            )

    def _scrape_carrier(self, carrier_slug: str, db: impuls.DBConnection) -> None:
        for brand in self.endpoint.carrier_trains_lists(carrier_slug):
            self.logger.info("Scraping brand %s %r", brand["id"], brand["name"])
            db.raw_execute(
                "INSERT INTO routes (agency_id, route_id, short_name, long_name, type) "
                "VALUES ('0', ?, ?, '', 2)",
                (brand["id"], brand["name"]),
            )
            for i, carrier_train in enumerate(brand["trains"], start=1):
                if i % 500:
                    self.logger.log(
                        logging.INFO if i % 10 == 0 else logging.DEBUG,
                        "Processed %d/%d of %r trains",
                        i,
                        len(brand["trains"]),
                        brand["name"],
                    )
                self._scrape_carrier_train(carrier_train, brand["id"], db)

    def _scrape_carrier_train(
        self,
        carrier_train: api.CarrierTrain,
        route_id: int,
        db: impuls.DBConnection,
    ) -> None:
        trip_short_name = (
            f'{carrier_train["nr"]} {carrier_train["name"].title()}'
            if carrier_train["name"]
            else str(carrier_train["nr"])
        )
        for calendar in self.endpoint.train_calendars(carrier_train):
            train_date_map = self._reverse_date_train_map(calendar["date_train_map"])
            for train_id, dates in train_date_map.items():
                db.raw_execute("INSERT INTO calendars (calendar_id) VALUES (?)", (train_id,))
                db.raw_execute_many(
                    "INSERT INTO calendar_exceptions (calendar_id,date,exception_type) "
                    "VALUES (?, ?, 1)",
                    ((train_id, str(date)) for date in dates),
                )
                db.raw_execute(
                    "INSERT INTO trips (route_id,trip_id,calendar_id,short_name) VALUES (?,?,?,?)",
                    (route_id, train_id, train_id, trip_short_name),
                )
                self._scrape_train_stops(train_id, db)

    def _scrape_train_stops(self, train_id: int, db: impuls.DBConnection) -> None:
        previous_departure = TimePoint(seconds=0)
        data = self.endpoint.train(train_id)
        bus_departures = self._bus_departures(data)

        for sequence, stop in enumerate(data["stops"]):
            self._ensure_stop_exists(stop["station_id"], stop["station_name"], db)

            # Ensure no time travel
            arrival = stop["arrival"]
            while arrival < previous_departure:
                arrival += TimePoint(days=1)
            departure = stop["departure"]
            while departure < arrival:
                departure += TimePoint(days=1)
            previous_departure = departure

            db.raw_execute(
                "INSERT INTO stop_times (trip_id, stop_sequence, stop_id, arrival_time,"
                " departure_time, platform) VALUES (?,?,?,?,?,?)",
                (
                    train_id,
                    sequence,
                    stop["station_id"],
                    arrival.total_seconds(),
                    departure.total_seconds(),
                    "BUS" if sequence in bus_departures else stop["platform"],
                ),
            )

    def _ensure_stop_exists(self, id: int, name: str, db: impuls.DBConnection) -> None:
        if id not in self._added_stations:
            db.raw_execute("INSERT INTO stops (stop_id,name,lat,lon) VALUES (?,?,0,0)", (id, name))
            self._added_stations.add(id)

    def _bus_departures(self, data: api.Train) -> set[int]:
        if "ZKA" in (data["train"]["name"] or ""):
            return set(range(len(data["stops"])))

        name_to_idx = self._station_name_to_idx(data["stops"])
        bus_departures = set[int]()
        for attribute in data["train"]["train_attributes"]:
            if attribute.id in BUS_ATTRIBUTE_IDS:
                start = self.pick_attribute_idx(
                    data["train"]["id"],
                    attribute.id,
                    attribute.begin_station_name,
                    name_to_idx,
                    is_start=True,
                )
                end = self.pick_attribute_idx(
                    data["train"]["id"],
                    attribute.id,
                    attribute.begin_station_name,
                    name_to_idx,
                    is_start=True,
                )
                if start is not None and end is not None:
                    end_is_last = end == len(data["stops"]) - 1
                    if end_is_last:
                        end += 1  # mark last stop as "BUS" as well, it can't be a departure
                    bus_departures.update(range(start, end + end_is_last))

        return bus_departures

    def pick_attribute_idx(
        self,
        train_id: int,
        attribute_id: int,
        name: str,
        name_to_idx: Mapping[str, list[int]],
        is_start: bool,
    ) -> int | None:
        candidates = name_to_idx.get(name)
        if not candidates:
            self.logger.warn(
                "Train %d attribute %d references unknown station %r",
                train_id,
                attribute_id,
                name,
            )
            return None
        elif len(candidates) > 1:
            self.logger.warn(
                "Train %d attribute %d references ambiguous station %r",
                train_id,
                attribute_id,
                name,
            )
            return candidates[0 if is_start else -1]
        return candidates[0]

    @staticmethod
    def _station_name_to_idx(stops: Iterable[api.TrainStop]) -> defaultdict[str, list[int]]:
        name_to_idx = defaultdict[str, list[int]](list)
        for idx, stop in enumerate(stops):
            name_to_idx[stop["station_name"]].append(idx)
        return name_to_idx

    @staticmethod
    def _reverse_date_train_map(date_train_map: Mapping[str, int]) -> defaultdict[int, list[Date]]:
        train_date_map = defaultdict[int, list[Date]](list)
        for date_str, train_id in date_train_map.items():
            train_date_map[train_id].append(Date.from_ymd_str(date_str))
        return train_date_map
