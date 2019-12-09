import time

from azure.common.client_factory import get_client_from_auth_file
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import Factory


class DataFactory:
    def __init__(
        self,
        datafactory_name: str,
        resource_group_name: str,
        resource_group_location: str,
    ):
        self.datafactory_name = datafactory_name
        self.resource_group_name = resource_group_name
        self.resource_group_location = resource_group_location

    def client(self):
        return get_client_from_auth_file(DataFactoryManagementClient, auth_path='')

    def create(self):
        datafactory_resource = Factory(location=self.resource_group_location)
        datafactory = self.client().factories.create_or_update(self.resource_group_name,
            self.datafactory_name,
            datafactory_resource)

        while datafactory.provisioning_state != 'Succeeded':
            datafactory = self.client().factories.get(self.resource_group_name, self.datafactory_name)
            time.sleep(1)

        return datafactory
    
    def destroy(self):
        self.client().factories.delete(self.resource_group_name, self.datafactory_name)

    def create_pipeline(self, pipeline_name: str, pipeline: object):
        return self.client().pipelines.create_or_update(
            resource_group_name=self.resource_group_name,
            factory_name=self.datafactory_name,
            pipeline_name=pipeline_name,
            pipeline=pipeline
        )

    def run_pipeline(self, pipeline_name: str):
        return self.client().pipelines.create_run(
            resource_group_name=self.resource_group_name,
            factory_name=self.datafactory_name,
            pipeline_name=pipeline_name
        )

    def create_trigger(self, trigger_name: str, properties: object):
        self.client().triggers.stop(
            resource_group_name=self.resource_group_name,
            factory_name=self.datafactory_name,
            trigger_name=trigger_name,
            properties=properties
        )

        return self.client().triggers.create_or_update(
            resource_group_name=self.resource_group_name,
            factory_name=self.datafactory_name,
            trigger_name=trigger_name,
            properties=properties
        )

    def run_trigger(self, trigger_name: str):
        return self.client().triggers.start(
            resource_group_name=self.resource_group_name,
            factory_name=self.datafactory_name,
            trigger_name=trigger_name,
            raw=True
        )
