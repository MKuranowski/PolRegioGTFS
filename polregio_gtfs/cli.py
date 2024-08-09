import argparse
import logging

import impuls
from impuls.model import Agency

from .load_station_data import LoadStationData
from .scrape_api import ScrapeAPI
from .split_bus_legs import SplitBusLegs


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
                impuls.tasks.ExecuteSQL(
                    task_name="NormalizePlatforms",
                    statement=(
                        "UPDATE stop_times SET platform = CASE "
                        "  WHEN platform = 'I' THEN '1' "
                        "  WHEN platform = 'Ia' THEN '1a' "
                        "  WHEN platform = 'Ib' THEN '1b' "
                        "  WHEN platform = 'II' THEN '2' "
                        "  WHEN platform = 'IIa' THEN '2a' "
                        "  WHEN platform = 'III' THEN '3' "
                        "  WHEN platform = 'IIIa' THEN '3a' "
                        "  WHEN platform = 'IV' THEN '4' "
                        "  WHEN platform = 'IVa' THEN '4a' "
                        "  WHEN platform = 'V' THEN '5' "
                        "  WHEN platform = 'VI' THEN '6' "
                        "  WHEN platform = 'VII' THEN '7' "
                        "  WHEN platform = 'VIII' THEN '8' "
                        "  WHEN platform = 'IX' THEN '9' "
                        "  WHEN platform = 'X' THEN '10' "
                        "  WHEN platform = 'XI' THEN '11' "
                        "  ELSE platform "
                        "END"
                    ),
                ),
                impuls.tasks.ExecuteSQL(
                    task_name="NormalizeTrainNameSKA",
                    statement=r"UPDATE trips SET short_name = re_sub('Ska', 'SKA', short_name)",
                ),
                impuls.tasks.ExecuteSQL(
                    task_name="NormalizeTrainNamePKM",
                    statement=r"UPDATE trips SET short_name = re_sub('Pkm', 'PKM', short_name)",
                ),
                impuls.tasks.ExecuteSQL(
                    task_name="NormalizeTrainNameZKA",
                    statement=r"UPDATE trips SET short_name = re_sub('Zka', 'ZKA', short_name)",
                ),
                impuls.tasks.ExecuteSQL(
                    task_name="NormalizeTrainNameI",
                    statement=r"UPDATE trips SET short_name = re_sub('\bI\b', 'i', short_name)",
                ),
                impuls.tasks.GenerateTripHeadsign(),
                SplitBusLegs(),
                impuls.tasks.ModifyRoutesFromCSV("routes.csv", must_curate_all=True),
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
                            "platform",
                        ),
                        "calendar_dates": ("service_id", "date", "exception_type"),
                        "transfers": (
                            "from_stop_id",
                            "to_stop_id",
                            "from_trip_id",
                            "to_trip_id",
                            "transfer_type",
                        ),
                    },
                    target="polregio.zip",
                ),
            ],
            resources={
                "pl_rail_map.osm": impuls.HTTPResource.get(
                    "https://raw.githubusercontent.com/MKuranowski/PLRailMap/master/plrailmap.osm"
                ),
                "routes.csv": impuls.LocalResource("routes.csv"),
            },
            options=options,
        )

    def before_run(self) -> None:
        logging.getLogger("urllib3").setLevel(logging.INFO)


def main() -> None:
    PolRegioGTFS().run()
