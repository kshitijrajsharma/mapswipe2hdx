import logging
import os
import pathlib
import re
import shutil
import zipfile
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
import requests
import yaml
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from tqdm import tqdm


class Config:
    def __init__(self, config_input):
        self.config = self.load_config(config_input)
        self.HDX_SITE = os.getenv("HDX_SITE", self.config.get("hdx_site", "demo"))
        self.HDX_API_KEY = os.getenv("HDX_API_KEY", self.config.get("hdx_api_key"))
        self.HDX_OWNER_ORG = os.getenv(
            "HDX_OWNER_ORG", self.config.get("hdx_owner_org")
        )
        self.HDX_MAINTAINER = os.getenv(
            "HDX_MAINTAINER", self.config.get("hdx_maintainer")
        )
        self.DATASET_NAME = self.config.get("dataset_name", "MapSwipe Results")
        self.DATASET_DESCRIPTION = self.config.get(
            "dataset_description",
            "MapSwipe results aggregated from multiple projects.",
        )
        self.DATASET_PREFIX = self.config.get(
            "dataset_prefix", self.DATASET_NAME.lower().replace(" ", "_")
        )
        self.DATASET_LOCATION = self.config.get("dataset_location", "Global")
        self.DATASET_FREQUENCY = self.config.get("dataset_frequency", "As Needed")
        self.DATASET_TAGS = self.config.get("dataset_tags", ["geodata"])
        self.DATASET_LICENSE = self.config.get("dataset_license", "hdx-odc-odbl")
        self.LOG_LEVEL = self.config.get("log_level", "INFO")
        self.LOG_FORMAT = self.config.get(
            "log_format",
            "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
        )
        self.PROJECTS = self.config.get("projects", [])
        self.FILE_FORMATS = self.config.get("file_formats", ["geojson"])

        self.setup_logging()
        self.setup_config()

    def load_config(self, config_input):
        if isinstance(config_input, str):
            if os.path.isfile(config_input):
                with open(config_input, "r") as file:
                    return yaml.safe_load(file)
            else:
                return yaml.safe_load(config_input)
        elif isinstance(config_input, dict):
            return config_input
        else:
            raise ValueError("Config must be a path, YAML string, or dictionary")

    def setup_logging(self):
        logging.basicConfig(level=self.LOG_LEVEL, format=self.LOG_FORMAT)
        self.logger = logging.getLogger(__name__)

    def setup_config(self):
        if not (self.HDX_API_KEY and self.HDX_OWNER_ORG and self.HDX_MAINTAINER):
            raise ValueError(
                "HDX credentials (API key, owner org, maintainer) are required"
            )

        self.HDX_URL_PREFIX = Configuration.create(
            hdx_site=self.HDX_SITE,
            hdx_key=self.HDX_API_KEY,
            user_agent="HDXPythonLibrary/6.3.4",
        )
        self.logger.info(f"Using HDX site: {self.HDX_URL_PREFIX}")


class MapSwipeDataFetcher:
    def __init__(self, config):
        self.config = config
        self.logger = config.logger

    def fetch_project_data(self, project_id):
        yes_maybe_url = (
            f"https://apps.mapswipe.org/api/yes_maybe/yes_maybe_{project_id}.geojson"
        )
        aoi_url = f"https://apps.mapswipe.org/api/project_geometries/project_geom_{project_id}.geojson"

        try:
            yes_maybe_response = requests.get(yes_maybe_url)
            yes_maybe_response.raise_for_status()
            data_yes_maybe = gpd.read_file(yes_maybe_response.text)

            aoi_response = requests.get(aoi_url)
            aoi_response.raise_for_status()
            aoi_data = gpd.read_file(aoi_response.text)

            return data_yes_maybe, aoi_data
        except Exception as e:
            self.logger.error(
                f"Failed to fetch data for project {project_id}: {str(e)}"
            )
            return None, None

    def extract_project_id(self, project_id_or_url):
        if isinstance(project_id_or_url, str) and project_id_or_url.startswith("http"):
            match = re.search(r"projects/([-\w]+)/?$", project_id_or_url)
            if match:
                return match.group(1)
            else:
                self.logger.warning(
                    f"Could not extract project ID from URL: {project_id_or_url}"
                )
                return project_id_or_url
        return project_id_or_url


class MapSwipeDataAggregator:
    def __init__(self, config):
        self.config = config
        self.logger = config.logger
        self.data_fetcher = MapSwipeDataFetcher(config)

    def aggregate_project_data(self):
        combined_yes_maybe = gpd.GeoDataFrame()
        combined_aoi = gpd.GeoDataFrame()

        for project in tqdm(self.config.PROJECTS, desc="Fetching MapSwipe Projects"):
            project_id = self.data_fetcher.extract_project_id(project["project_id"])
            project_name = project.get("name", f"Project {project_id}")
            self.logger.info(f"Fetching project: {project_name} (ID: {project_id})")

            data_yes_maybe, aoi_data = self.data_fetcher.fetch_project_data(project_id)
            if data_yes_maybe is not None and aoi_data is not None:
                if "project_name" not in data_yes_maybe.columns:
                    data_yes_maybe["project_name"] = project_name
                # if "project_uid" not in data_yes_maybe.columns:
                #     data_yes_maybe["project_uid"] = str(project_id)

                if combined_yes_maybe.empty:
                    combined_yes_maybe = data_yes_maybe.copy()
                else:
                    combined_yes_maybe = pd.concat(
                        [combined_yes_maybe, data_yes_maybe], ignore_index=True
                    )

                if "project_name" not in aoi_data.columns:
                    aoi_data["project_name"] = project_name

                # if "project_uid" not in aoi_data.columns:
                #     aoi_data["project_uid"] = str(project_id)

                if combined_aoi.empty:
                    combined_aoi = aoi_data.copy()
                else:
                    combined_aoi = pd.concat(
                        [combined_aoi, aoi_data], ignore_index=True
                    )
        return combined_yes_maybe, combined_aoi


class HDXDatasetCreator:
    def __init__(self, config):
        self.config = config
        self.logger = config.logger

    def attach_project_links_to_description(self, base_description=None):
        description = base_description or self.config.DATASET_DESCRIPTION

        project_links = []
        for project in self.config.PROJECTS:
            project_id = project.get("project_id", "")
            project_name = project.get("name", f"Project {project_id}")

            if isinstance(project_id, str) and project_id.startswith("http"):
                extracted_id = re.search(r"projects/([-\w]+)/?$", project_id)
                if extracted_id:
                    project_id = extracted_id.group(1)
            elif isinstance(project_id, str) and project_id.startswith("-"):
                pass

            project_url = f"https://mapswipe.org/en/projects/{project_id}/"
            project_links.append(f"- [{project_name}]({project_url})")

        if project_links:
            if description and not description.endswith("\n\n"):
                description += "\n\n"

            description += " Source MapSwipe Projects\n\n"
            description += "\n".join(project_links)
        return description

    def create_and_upload_dataset(self, combined_yes_maybe, combined_aoi):
        dataset_args = {
            "title": self.config.DATASET_NAME,
            "name": self.config.DATASET_PREFIX,
            "notes": self.attach_project_links_to_description(
                self.config.DATASET_DESCRIPTION
            ),
            "private": False,
            "dataset_source": "MapSwipe",
            "methodology": "Other",
            "methodology_other": "Human validated results from MapSwipe app",
            "owner_org": self.config.HDX_OWNER_ORG,
            "maintainer": self.config.HDX_MAINTAINER,
            "license_id": self.config.DATASET_LICENSE,
            "subnational": False,
        }

        dataset = Dataset(dataset_args)
        dataset.set_time_period(datetime.now())
        dataset.set_expected_update_frequency(self.config.DATASET_FREQUENCY)
        dataset.add_other_location(self.config.DATASET_LOCATION)
        for tag in self.config.DATASET_TAGS:
            dataset.add_tag(tag)

        zip_paths = []

        for fmt in self.config.FILE_FORMATS:
            try:
                dir_path = f"{os.getcwd()}/{self.config.DATASET_PREFIX}_{fmt}"
                os.makedirs(dir_path, exist_ok=True)

                if fmt == "shp":
                    self.export_shapefile(
                        combined_yes_maybe, "results_yes_maybe", dir_path
                    )
                    self.export_shapefile(combined_aoi, "aois", dir_path)
                else:
                    driver = self.get_driver_for_format(fmt)
                    yes_maybe_path = f"{dir_path}/results_yes_maybe.{fmt}"
                    combined_yes_maybe.to_file(yes_maybe_path, driver=driver)
                    aoi_path = f"{dir_path}/aois.{fmt}"
                    combined_aoi.to_file(aoi_path, driver=driver)

                zip_name = f"{self.config.DATASET_PREFIX}_{fmt}.zip".lower().replace(
                    " ", "_"
                )
                zip_path = self.file_to_zip(dir_path, zip_name)
                zip_paths.append(zip_path)

                fmt_display = self.get_format_display_name(fmt)

                resource_yes_maybe = Resource(
                    {
                        "name": f"{self.config.DATASET_PREFIX}_results_yes_maybe.{fmt}",
                        "description": f"Combined results from MapSwipe projects in {fmt_display} format",
                    }
                )
                resource_yes_maybe.set_format(fmt)
                resource_yes_maybe.set_file_to_upload(zip_path)
                dataset.add_update_resource(resource_yes_maybe)

            except Exception as e:
                self.logger.error(f"Error exporting to {fmt}: {str(e)}")
                raise e

        dataset.create_in_hdx(allow_no_resources=True)
        dataset.update_in_hdx()
        return dataset.get_hdx_url()

    def get_driver_for_format(self, fmt):
        """Return the appropriate driver name for a given file format"""
        format_drivers = {
            "geojson": "GeoJSON",
            "gpkg": "GPKG",
            "kml": "KML",
            "shp": "ESRI Shapefile",
        }
        return format_drivers.get(fmt.lower(), fmt.upper())

    def get_format_display_name(self, fmt):
        """Return a user-friendly display name for a format"""
        format_names = {
            "geojson": "GeoJSON",
            "gpkg": "GeoPackage",
            "kml": "KML",
            "shp": "Shapefile",
        }
        return format_names.get(fmt.lower(), fmt.upper())

    def export_shapefile(self, gdf, name, dir_path):
        geom_types = gdf.geometry.type.unique()
        for geom_type in geom_types:
            geom_gdf = gdf[gdf.geometry.type == geom_type]
            geom_gdf.to_file(
                f"{dir_path}/{name}_{geom_type}.shp", driver="ESRI Shapefile"
            )

    def file_to_zip(self, working_dir, zip_path):
        buffer_size = 4 * 1024 * 1024

        with zipfile.ZipFile(
            zip_path,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            allowZip64=True,
            compresslevel=1,
        ) as zf:
            for file_path in pathlib.Path(working_dir).iterdir():
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                if file_size_mb > 100:
                    with open(file_path, "rb") as f:
                        with zf.open(file_path.name, "w", force_zip64=True) as dest:
                            shutil.copyfileobj(f, dest, buffer_size)
                else:
                    zf.write(file_path, arcname=file_path.name)

            utc_now = datetime.now(timezone.utc)
            utc_offset = utc_now.strftime("%z")
            readme_content = (
                f"Exported using MapSwipe Data Aggregator\n"
                f"Timestamp (UTC{utc_offset}): {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Data Source: https://mapswipe.org/\n"
                f"Dataset: {self.config.DATASET_NAME}\n"
            )
            zf.writestr("Readme.txt", readme_content)

        shutil.rmtree(working_dir)
        return zip_path


def main():
    config_path = "config.yaml"
    config = Config(config_path)

    data_aggregator = MapSwipeDataAggregator(config)
    dataset_creator = HDXDatasetCreator(config)

    combined_yes_maybe, combined_aoi = data_aggregator.aggregate_project_data()

    if not combined_yes_maybe.empty and not combined_aoi.empty:
        dataset_creator.create_and_upload_dataset(combined_yes_maybe, combined_aoi)
    else:
        config.logger.error(
            "No valid data fetched from the provided MapSwipe projects."
        )


if __name__ == "__main__":
    main()
