# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

import json
from csv import DictReader
from typing import Callable, Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from .utils.properties import add_pid, create_pid
from .utils.reshape import reshape


def run(
    csv_file: str,
    column_names: Optional[list[str]] = None,
    output_prefix: str = "output_file",
    output_suffix: str = ".jsonl",
    beam_options: Optional[PipelineOptions] = None,
) -> None:
    with beam.Pipeline(options=beam_options) as pipeline:
        df = (
            pipeline
            | "Load CSV file" >> beam.Create(DictReader(open(csv_file), fieldnames=column_names))
            | "Add Property ID Data" >> beam.GroupBy(create_pid)
            | "Reshape into appropriate format" >> beam.Map(reshape)
            | "Change into JSON text" >> beam.Map(json.dumps)
            | "Save Output" >> beam.io.textio.WriteToText(
                file_path_prefix=output_prefix,
                file_name_suffix=output_suffix,
            )
        )
