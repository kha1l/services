"""Общие resource_defs для `servises`."""

from resources import db_resource, dodo_api_resource, rabbitmq_resource


COMMON_RESOURCE_DEFS = {
    "db": db_resource,
    "dodo_api": dodo_api_resource,
    "rabbitmq": rabbitmq_resource,
}
