from datafactory import DataFactory

from azure.mgmt.datafactory.models import ActivityDependency
from azure.mgmt.datafactory.models import LinkedServiceReference
from azure.mgmt.datafactory.models import AzureSqlDWLinkedService
from azure.mgmt.datafactory.models import SqlServerStoredProcedureActivity


class RebuildIndexActivities:
    def __init__(
        self,
        datafactory_name: str,
        resource_group_name: str,
        resource_group_location: str,
        datafactory_client: object,
        connection_string: str,
        encrypted_credential: str
    ):
        self.datafactory_name = datafactory_name
        self.resource_group_name = resource_group_name
        self.resource_group_location = resource_group_location
        self.datafactory_client = datafactory_client
        self.connection_string = connection_string
        self.encrypted_credential = encrypted_credential

    def get(self):
        return [
            self.__rebuild_indexes(),
            self.__create_statistics()
        ]

    def create_linked_service(self):
        azure_sql_dw = AzureSqlDWLinkedService(
            connection_string=self.connection_string,
            encrypted_credential=self.encrypted_credential
        )

        self.datafactory_client.linked_services.create_or_update(
            self.resource_group_name, 
            self.datafactory_name, 
            'Warehouse',
            azure_sql_dw
        )

    @property
    def linked_service_reference(self):
        return LinkedServiceReference(reference_name='Warehouse')

    @property
    def depends_on_rebuild_indexes(self):
        return ActivityDependency(
            activity='RebuildIndexes',
            dependency_conditions=['Succeeded'],
            additional_properties={}
        )

    def __rebuild_indexes(self):
        return SqlServerStoredProcedureActivity(
            name='RebuildIndexes',
            type='SqlServerStoredProcedure',
            stored_procedure_name='[dbo].[spRebuildIndexes]',
            linked_service_name=self.linked_service_reference,
            stored_procedure_parameters={}
        )

    def __create_statistics(self):
        return SqlServerStoredProcedureActivity(
            name='CreateStatistics',
            type='SqlServerStoredProcedure',
            stored_procedure_name='[dbo].[spCreateStatistics]',
            depends_on=[
                self.depends_on_rebuild_indexes
            ],
            linked_service_name=self.linked_service_reference,
            stored_procedure_parameters={}
        )
