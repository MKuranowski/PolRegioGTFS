from copy import copy
from typing import NamedTuple, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import StopTime, Transfer, Trip


class Leg(NamedTuple):
    stop_times: list[StopTime]
    is_bus: bool


class SplitBusLegs(Task):
    def __init__(self) -> None:
        super().__init__()
        self.added_routes = set[str]()

    def execute(self, r: TaskRuntime) -> None:
        self.added_routes.clear()
        to_process = [cast(str, i[0]) for i in r.db.raw_execute("SELECT trip_id FROM trips")]
        with r.db.transaction():
            for i, trip_id in enumerate(to_process, start=1):
                self.process_train(trip_id, r.db)
                if i % 500 == 0:
                    self.logger.info("Processed %d/%d trains", i, len(to_process))

    def process_train(self, trip_id: str, db: DBConnection) -> None:
        train = db.retrieve_must(Trip, trip_id)
        stop_times = list(
            db.typed_out_execute("SELECT * FROM stop_times WHERE trip_id=?", StopTime, (trip_id,))
        )

        legs = compute_legs(stop_times)

        if "ZKA" in train.short_name or (len(legs) == 1 and legs[0].is_bus):
            self.replace_train_by_bus(train, db)
        elif len(legs) > 1:
            self.replace_train_by_legs(train, legs, db)
        # else - one train leg, nothing to do

    def replace_train_by_bus(self, train: Trip, db: DBConnection) -> None:
        bus_route_id = self.get_bus_route(train.route_id, db)
        db.raw_execute("UPDATE trips SET route_id = ? WHERE trip_id = ?", (bus_route_id, train.id))

    def replace_train_by_legs(self, train: Trip, legs: list[Leg], db: DBConnection) -> None:
        bus_route_id = self.get_bus_route(train.route_id, db)
        db.raw_execute("DELETE FROM trips WHERE trip_id = ?", (train.id,))

        for idx, (leg, is_bus) in enumerate(legs):
            # Insert trip representing this leg
            trip = copy(train)
            trip.id = f"{train.id}_{idx}"
            if is_bus:
                trip.route_id = bus_route_id
            db.create(trip)

            # Insert stop_times of leg
            for stop_time in leg:
                stop_time.trip_id = trip.id
                db.create(stop_time)

            # Insert transfer between this and previous leg
            if idx != 0:
                db.create(
                    Transfer(
                        from_stop_id=leg[-1].stop_id,
                        to_stop_id=leg[-1].stop_id,
                        from_trip_id=f"{train.id}_{idx-1}",
                        to_trip_id=trip.id,
                        type=Transfer.Type.TIMED,
                    )
                )

    def get_bus_route(self, route_id: str, db: DBConnection) -> str:
        bus_route_id = route_id + "BUS"
        if bus_route_id not in self.added_routes:
            self.added_routes.add(bus_route_id)
            db.raw_execute(
                "INSERT INTO routes (agency_id,route_id,short_name,long_name,type) "
                "VALUES ('0',?,'','',3)",
                (bus_route_id,),
            )
        return bus_route_id


def compute_legs(stop_times: list[StopTime]) -> list[Leg]:
    legs = list[Leg]()
    leg = list[StopTime]()
    previous_is_bus = stop_times[0].platform == "BUS"

    for stop_time in stop_times:
        current_is_bus = stop_time.platform == "BUS"

        if previous_is_bus != current_is_bus:
            if leg:
                leg.append(arrival_only(stop_time, previous_is_bus))
                legs.append(Leg(leg, previous_is_bus))

            leg = [departure_only(stop_time, current_is_bus)]
            previous_is_bus = current_is_bus
        else:
            leg.append(stop_time)

    if len(leg) > 1:
        legs.append(Leg(leg, previous_is_bus))

    return legs


def arrival_only(st: StopTime, is_bus: bool) -> StopTime:
    new = copy(st)
    new.departure_time = new.arrival_time
    if is_bus:
        new.platform = "BUS"
    elif new.platform == "BUS":
        new.platform = ""
    return new


def departure_only(st: StopTime, is_bus: bool) -> StopTime:
    new = copy(st)
    new.arrival_time = new.departure_time
    if is_bus:
        new.platform = "BUS"
    elif new.platform == "BUS":
        new.platform = ""
    return new
