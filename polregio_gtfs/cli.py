import argparse
import logging

import impuls
from impuls.model import Agency

from .load_station_data import LoadStationData
from .scrape_api import ScrapeAPI


class PolRegioGTFS(impuls.App):
    def prepare(self, args: argparse.Namespace, options: impuls.PipelineOptions) -> impuls.Pipeline:
        return impuls.Pipeline(
            tasks=[
                impuls.tasks.AddEntity(
                    Agency(
                        id="0",
                        name="PolRegio",
                        url="https://polregio.pl/",
                        timezone="Europe/Warsaw",
                        lang="pl",
                        phone="+48703202020",
                    ),
                    task_name="AddAgency",
                ),
                ScrapeAPI(),
                LoadStationData(),
                impuls.tasks.GenerateTripHeadsign(),
                impuls.tasks.SaveGTFS(
                    headers={
                        "agency": (
                            "agency_id",
                            "agency_name",
                            "agency_url",
                            "agency_timezone",
                            "agency_lang",
                            "agency_phone",
                        ),
                        "stops": ("stop_id", "stop_name", "stop_lat", "stop_lon"),
                        "routes": (
                            "agency_id",
                            "route_id",
                            "route_short_name",
                            "route_long_name",
                            "route_type",
                        ),
                        "trips": (
                            "route_id",
                            "trip_id",
                            "service_id",
                            "trip_headsign",
                            "trip_short_name",
                        ),
                        "stop_times": (
                            "trip_id",
                            "stop_sequence",
                            "stop_id",
                            "arrival_time",
                            "departure_time",
                        ),
                        "calendar_dates": ("service_id", "date", "exception_type"),
                    },
                    target="polregio.zip",
                ),
            ],
            resources={
                "pl_rail_map.osm": impuls.HTTPResource.get(
                    "https://raw.githubusercontent.com/MKuranowski/PLRailMap/master/plrailmap.osm"
                ),
            },
            options=options,
        )

    def before_run(self) -> None:
        logging.getLogger("urllib3").setLevel(logging.INFO)


def main() -> None:
    PolRegioGTFS().run()