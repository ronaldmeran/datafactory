#!/usr/bin/env python3
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
        start_time='2019-08-29T11:43:00',
        end_time='2019-09-01T05:00:00',
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
