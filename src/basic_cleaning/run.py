#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import argparse
from cmath import log
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info("Downloading artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    # Load data
    df = pd.read_csv(artifact_local_path)

    # Drop outliers
    logger.info("Dropping data not between min_price and max_price")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # Drop boundaries data
    logger.info("Dropping data not in NYC")
    long_min = -74.25
    long_max = -73.50
    lat_min = 40.5
    lat_max = 41.2
    idx = df['longitude'].between(long_min, -long_max) & df['latitude'].between(lat_min, lat_max)
    df = df[idx].copy()

    # Convert data type
    logger.info("Converting last_review data type to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Save data
    logger.info("Saving data to csv")
    df.to_csv(args.output_artifact, index=False)

    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This steps cleans the data")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="output type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="output description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="miniumn price",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="maximum price",
        required=True
    )


    args = parser.parse_args()

    go(args)
