#!/usr/bin/env python3
import time
import json
import argparse

from datetime import datetime, timedelta
from azure.mgmt.datafactory.models import PipelineResource
from azure.mgmt.datafactory.models import CreateRunResponse
from azure.mgmt.datafactory.models import PipelineReference
from azure.mgmt.datafactory.models import ScheduleTrigger
from azure.mgmt.datafactory.models import TriggerPipelineReference
from azure.mgmt.datafactory.models import ScheduleTriggerRecurrence

from configs.azure_config import azure
from monitoring import print_activity_run_details
from datafactory import DataFactory
from trigger.schedule import daily_trigger
from activities.rebuild_indexes import RebuildIndexActivities


argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument('datafactory_name', type=str, help='(Data Factory Name).')
argparser.add_argument('resource_group_name', type=str, help='(Resource Group Name).')
argparser.add_argument('resource_group_location', type=str, help='(Resource Group Location).')
args = argparser.parse_args()


def main():
    adf = DataFactory(args.datafactory_name, args.resource_group_name, args.resource_group_location)
    activities = RebuildIndexActivities(
        args.datafactory_name,
        args.resource_group_name,
        args.resource_group_location,
        adf.client(),
        azure.connection_string,
        azure.encrypted_credential
    )

    # Create or Update Linked Services
    activities.create_linked_service()
    
    # Define activities
    pipeline = PipelineResource(
        activities=activities.get(), 
        parameters={}
    )
    
    # Create pipeline
    pipeline_name = 'RebuildIndexes'
    adf.create_pipeline(pipeline_name, pipeline)
    pipeline_run = adf.run_pipeline(pipeline_name)

    # Add the pipeline to daily trigger
    daily_trigger(adf, pipeline_name)

    # Monitoring
    time.sleep(30)
    pipeline_run_response = adf.client().pipeline_runs.get(
        resource_group_name=args.resource_group_name,
        factory_name=args.datafactory_name,
        run_id=pipeline_run.run_id
    )

    print("Pipeline run status: {}".format(pipeline_run_response.status))
    activity_runs_paged = adf.client().activity_runs.query_by_pipeline_run(
        args.resource_group_name, 
        args.datafactory_name, 
        pipeline_run_response.run_id, 
        {
            'lastUpdatedBefore': datetime.now() - timedelta(1),
            'lastUpdatedAfter': datetime.today() + timedelta(1),
        }
    )

    # Bug in activity_runs, no model for list_by_pipeline_run
    # query_by_pipeline_run is used instead, however, ActivityRunQueryResponse
    # is used in the model instead of ActivityRun, so below can't be used to print status
    # print_activity_run_details(activity_runs_paged[0])
    print(vars(activity_runs_paged))


if __name__ == '__main__':
    main()
