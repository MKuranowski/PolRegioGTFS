import argparse
import impuls

class PolRegioGTFS(impuls.App):
    def prepare(self, args: argparse.Namespace, options: impuls.PipelineOptions) -> impuls.Pipeline:
        return impuls.Pipeline(
            tasks=[],
        )


def main() -> None:
    PolRegioGTFS().run()
