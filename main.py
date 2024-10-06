# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from apache_beam.options.pipeline_options import PipelineOptions

from gov_prop_sales import app

if __name__ == "__main__":
    import argparse
    import logging

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv-file",
        default="",
        help="Path to CSV file.",
    )
    args, beam_args = parser.parse_known_args()

    column_names = [
        "tid",
        "price",
        "date",
        "postcode",
        "property_type",
        "is_new",
        "freehold_or_leasehold",
        "paon",
        "saon",
        "street",
        "locality",
        "town_or_city",
        "district",
        "county",
        "ppd_category_type",
        "record_status",
    ]

    beam_options = PipelineOptions(save_main_session=True, setup_file="./setup.py")
    app.run(
        csv_file=args.csv_file,
        column_names=column_names,
        beam_options=beam_options,
    )
