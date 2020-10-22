#!/usr/bin/env python3
import logging


def print_activity_run_details(activity_run):
    """Print activity run details."""
    logging.info("Activity run details")
    logging.info("Activity run status: {}".format(activity_run.status))
    if activity_run.status == 'Succeeded':
        logging.info("Number of bytes read: {}".format(
            activity_run.output['dataRead']))
        logging.info("Number of bytes written: {}".format(
            activity_run.output['dataWritten']))
        logging.info("Copy duration: {}".format(
            activity_run.output['copyDuration']))
    else:
        logging.error("Errors: {}".format(activity_run.error['message']))
