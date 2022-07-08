"""Script for running the spark pieline and save the output CSVs."""

import argparse
from pathlib import Path
import datetime

from product_integrator.pipeline.utils import init_spark_session
from product_integrator.preprocessing import preprocess_supplier_data
from product_integrator.normalisation import normalise_preprocessed_supplier_data
from product_integrator.extraction import extraction_from_normalised_supplier_data
from product_integrator.integration import integrate_supplier_data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--task",
        type=str,
        help="task are: 'preprocessing', 'normalisation', 'extraction' and 'intergration' ",
        required=True,
    )

    parser.add_argument(
        "--output_path",
        type=str,
        help="Generated image output path including the filename",
        required=True,
    )

    args = parser.parse_args()
    return args


def main():
    """Entrypoint function"""

    # parse function parameters
    args = parse_args()
    spark = init_spark_session()

    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    timestamped_output_path = args.output_path + timestamp

    Path(timestamped_output_path).mkdir(parents=True, exist_ok=True)

    if args.task.lower() in [
        "preprocessing",
        "normalisation",
        "extraction",
        "integration",
    ]:
        preprocessed_df = preprocess_supplier_data(spark, timestamped_output_path, True)

    if args.task.lower() in ["normalisation", "extraction", "integration"]:
        normalised_df = normalise_preprocessed_supplier_data(
            preprocessed_df, timestamped_output_path, True
        )

    if args.task.lower() in ["extraction", "integration"]:
        extracted_df = extraction_from_normalised_supplier_data(
            normalised_df, timestamped_output_path, True
        )

    if args.task.lower() in ["integration"]:
        integrate_supplier_data(extracted_df, timestamped_output_path, True)


if __name__ == "__main__":
    main()
