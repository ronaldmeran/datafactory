#!/usr/bin/env python3
import datetime

from azure.mgmt.datafactory.models import PipelineReference
from azure.mgmt.datafactory.models import ScheduleTrigger
from azure.mgmt.datafactory.models import TriggerPipelineReference
from azure.mgmt.datafactory.models import ScheduleTriggerRecurrence


def daily_trigger(adf, pipeline_name):
    # Create a trigger
    trigger_name = 'DailyTrigger'
    scheduler_recurrence = ScheduleTriggerRecurrence(
        frequency='Day', 
        interval='15',
        start_time=datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        end_time=None,
        time_zone='UTC'
    )
    
    pipeline_reference = PipelineReference(reference_name=pipeline_name)
    trigger_properties = ScheduleTrigger(
        description='Schedule Trigger', 
        pipelines=[
            TriggerPipelineReference(pipeline_reference=pipeline_reference, parameters={})
        ], 
        recurrence=scheduler_recurrence)
    
    # Create and Run Trigger Schedule
    adf.create_trigger(trigger_name, trigger_properties)
    adf.run_trigger(trigger_name)
