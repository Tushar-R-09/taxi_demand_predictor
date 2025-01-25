from typing import Optional, List
from dataclasses import dataclass

import hsfs
import hopsworks

import src.config as config

@dataclass
class FeatureGroupConfig:
    name: str
    version: int
    description: str
    primary_key: List[str]
    event_time: str
    online_enabled: Optional[bool] = False

@dataclass
class FeatureViewConfig:
    name: str
    version: int
    feature_group: FeatureGroupConfig

def get_feature_store() -> hsfs.feature_store.FeatureStore:
    """Connects to Hopsworks and returns a pointer to the feature store

    Returns:
        hsfs.feature_store.FeatureStore: pointer to the feature store
    """
    print(config.HOPSWORKS_PROJECT_NAME, config.HOPSWORKS_API_KEY)
    project = hopsworks.login(
        project=config.HOPSWORKS_PROJECT_NAME,
        api_key_value=config.HOPSWORKS_API_KEY
    )
    return project.get_feature_store()

# TODO: remove this function, and use get_or_create_feature_group instead
def get_feature_group(
    name: str,
    version: Optional[int] = 1
    ) -> hsfs.feature_group.FeatureGroup:
    """Connects to the feature store and returns a pointer to the given
    feature group `name`

    Args:
        name (str): name of the feature group
        version (Optional[int], optional): _description_. Defaults to 1.

    Returns:
        hsfs.feature_group.FeatureGroup: pointer to the feature group
    """
    return get_feature_store().get_feature_group(
        name=name,
        version=version,
    )

def get_or_create_feature_group(
    feature_group_metadata: FeatureGroupConfig
) -> hsfs.feature_group.FeatureGroup:
    """Connects to the feature store and returns a pointer to the given
    feature group `name`

    Args:
        name (str): name of the feature group
        version (Optional[int], optional): _description_. Defaults to 1.

    Returns:
        hsfs.feature_group.FeatureGroup: pointer to the feature group
    """
    return get_feature_store().get_or_create_feature_group(
        name=feature_group_metadata.name,
        version=feature_group_metadata.version,
        description=feature_group_metadata.description,
        primary_key=feature_group_metadata.primary_key,
        event_time=feature_group_metadata.event_time,
        online_enabled=feature_group_metadata.online_enabled
    )

def get_or_create_feature_view(
    feature_view_metadata: FeatureViewConfig
) -> hsfs.feature_view.FeatureView:
    """
    Ensures a feature view is created or updated to reflect the latest data from the feature group.
    """
    # Get pointer to the feature store
    feature_store = get_feature_store()

    # Get pointer to the feature group
    feature_group = feature_store.get_feature_group(
        name=feature_view_metadata.feature_group.name,
        version=feature_view_metadata.feature_group.version
    )

    try:
        # Try to retrieve the feature view
        feature_view = feature_store.get_feature_view(
            name=feature_view_metadata.name,
            version=feature_view_metadata.version,
        )

        # If feature view exists, delete and recreate it to reflect the latest data
        feature_view.delete()
        feature_view = feature_store.create_feature_view(
            name=feature_view_metadata.name,
            version=feature_view_metadata.version,
            query=feature_group.select_all(),
        )
        
    except hsfs.client.exceptions.RestAPIError as e:
        # Handle the case where the feature view does not exist
        if "not found" in str(e).lower() or "does not exist" in str(e).lower():
            # Create the feature view if it doesn't exist
            feature_view = feature_store.create_feature_view(
                name=feature_view_metadata.name,
                version=feature_view_metadata.version,
                query=feature_group.select_all(),
            )
        else:
            # Log and raise other exceptions
            raise e

    return feature_view

