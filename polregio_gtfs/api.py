# © Copyright 2024 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import time
from typing import Any, NamedTuple, TypedDict

import requests
from impuls.model import TimePoint

# Found endpoints on https://bilety.polregio.pl/pl/:
# https://bilety.polregio.pl/pl/brands
# https://bilety.polregio.pl/pl/carriers
# https://bilety.polregio.pl/pl/carrier_trains_lists?carrier=CARRIER_SLUG
# https://bilety.polregio.pl/pl/stations
# https://bilety.polregio.pl/pl/station_departures/STATION_SLUG (seems to be down)
# https://bilety.polregio.pl/pl/station_arrivals/STATION_SLUG (seems to be down)
# https://bilety.polregio.pl/pl/train_calendars?brand=BRAND&nr=NR&name=NAME_IF_PRESENT
# https://bilety.polregio.pl/pl/trains/TRAIN_ID


class Brand(TypedDict):
    id: int
    name: str
    carrier_id: int


class Carrier(TypedDict):
    id: int
    name: str
    short_name: str
    slug: str
    update_date: str
    update_time: str


class CarrierTrain(TypedDict):
    nr: int
    param_nr: str
    name: str | None
    brand: str


class CarrierTrainsList(TypedDict):
    id: int
    name: str
    trains: list[CarrierTrain]
    signature: str


class Station(TypedDict):
    id: int
    ibnr: int
    name: str
    name_slug: str


class TrainCalendar(TypedDict):
    id: int
    train_nr: int
    train_name: str | None
    trainBrand: str
    dates: list[str]
    train_ids: list[int]
    date_train_map: dict[str, int]


class TrainAttribute(NamedTuple):
    id: int
    description: str
    begin_station_name: str
    end_station_name: str
    unknown_1: bool
    unknown_2: str


class TrainDetails(TypedDict):
    id: int
    train_nr: int
    name: str | None
    train_full_name: str
    run_desc: str
    carrier_id: int
    brand_id: int
    train_attributes: list[TrainAttribute]


class TrainStop(TypedDict):
    id: int
    station_id: int
    station_name: str
    station_slug: str
    station_ibnr: int
    train_id: int
    distance: int
    arrival: TimePoint
    departure: TimePoint
    position: int
    brand_id: int
    platform: str
    track: int | None
    entry_only: bool
    exit_only: bool


class Train(TypedDict):
    train: TrainDetails
    stops: list[TrainStop]


class Endpoint:
    def __init__(self, pause_s: float = 0.05) -> None:
        self.session = requests.Session()
        self.last_call: float = 0.0
        self.pause = pause_s

    def _wait_between_calls(self) -> None:
        now = time.monotonic()
        delta = now - self.last_call
        if delta < self.pause:
            time.sleep(self.pause - delta)
            self.last_call = time.monotonic()
        else:
            self.last_call = now

    def _do_call(self, request: requests.Request) -> Any:
        self._wait_between_calls()
        prepared = self.session.prepare_request(request)
        with self.session.send(prepared) as response:
            response.raise_for_status()
            return response.json()

    def call(self, path: str, **params: str) -> Any:
        request = requests.Request(
            method="GET",
            url=f"https://bilety.polregio.pl/pl/{path}",
            params=params,
        )
        retries = 3
        for retry in range(1, retries + 1):
            try:
                return self._do_call(request)
            except requests.HTTPError:
                if retry == retries:
                    raise

    def brands(self) -> list[Brand]:
        return self.call("brands")["brands"]

    def carriers(self) -> list[Carrier]:
        return self.call("carriers")["carriers"]

    def carrier_trains_lists(self, carrier_slug: str) -> list[CarrierTrainsList]:
        return self.call("carrier_trains_lists", carrier=carrier_slug)["carrier_trains_lists"]

    def stations(self) -> list[Station]:
        return self.call("stations")["stations"]

    def train_calendars(self, carrier_train: CarrierTrain) -> list[TrainCalendar]:
        params = {"brand": carrier_train["brand"], "nr": str(carrier_train["nr"])}
        if carrier_train["name"] is not None:
            params["name"] = carrier_train["name"]
        return self.call("train_calendars", **params)["train_calendars"]

    def train(self, id: int) -> Train:
        data = self.call(f"trains/{id}")
        data["train"]["train_attributes"] = [
            TrainAttribute(*i) for i in data["train"]["train_attributes"]
        ]
        for stop in data["stops"]:
            stop["arrival"] = self._api_time_to_time_point(stop["arrival"])
            stop["departure"] = self._api_time_to_time_point(stop["departure"])
        return data

    @staticmethod
    def _api_time_to_time_point(x: dict[str, int]) -> TimePoint:
        assert len(x) == 3
        return TimePoint(hours=x["hour"], minutes=x["minute"], seconds=x["second"])
