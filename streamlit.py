import os
import tempfile

import yaml

import streamlit as st
from mapswipe2hdx.app import Config, HDXDatasetCreator, MapSwipeDataAggregator

st.set_page_config(
    page_title="MapSwipe to HDX Publisher",
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    st.title("MapSwipe to HDX Publisher")
    st.markdown("""
    This application helps you publish MapSwipe project data to the Humanitarian Data Exchange (HDX).
    1. Upload your YAML configuration file
    2. Review and edit settings
    3. Process MapSwipe data
    4. Publish to HDX
    """)

    # Sidebar for configuration and processing
    st.sidebar.header("Configuration")

    # File upload
    uploaded_file = st.sidebar.file_uploader(
        "Upload your YAML configuration file", type=["yaml", "yml"]
    )

    # Initialize configuration
    config_data = None

    if uploaded_file is not None:
        try:
            config_data = yaml.safe_load(uploaded_file)
            st.sidebar.success("Configuration loaded successfully!")
        except Exception as e:
            st.sidebar.error(f"Error loading configuration: {str(e)}")
    else:
        st.sidebar.info("Please upload a YAML configuration file to continue.")
        # Show example configuration
        if st.sidebar.expander("Show example configuration").expanded:
            st.code(
                """
# MapSwipe to HDX Configuration

# HDX Account Settings
hdx_site: "demo"  # Use "demo" or "prod" for production
hdx_api_key: "your-api-key-here"  # Your HDX API key
hdx_owner_org: "your-owner-org-here"  # Your HDX organization ID
hdx_maintainer: "maintainer-here"  # Your HDX maintainer ID

# Dataset Information
dataset_name: "MapSwipe Results : Myanmar Suspected Buildings"
dataset_prefix: "mapswipe_myanmar_buildings"  # Prefix for dataset files, make sure it is unique and short
dataset_description: "MapSwipe results for suspected buildings in Myanmar. This dataset contains geospatial data collected through the MapSwipe platform, focusing on building identification in disaster-affected areas."
dataset_location: "Myanmar"  # Country or region
dataset_frequency: "As needed"  # Options: daily, weekly, monthly, quarterly, biannually, annually, as_needed
dataset_tags:
  - "geodata"
  - "infrastructure"
# Output Settings
file_formats:
  - "geojson"   # Standard GeoJSON format
  - "gpkg"      # GeoPackage format 
  - "kml"       # Google KML format

# MapSwipe Projects to Process
projects:
  - project_id: "https://mapswipe.org/en/projects/-OMTfDy03ThqukWPeBVp/"
    name: "Myanmar: Disaster Response 1"
  
  - project_id: "https://mapswipe.org/en/projects/-OMqANF17XQSJaxob4LN/"
    name: "Myanmar: Disaster Response 2"
  
  - project_id: "-OMunaJmhQt4kOIb2gGH"  # Project id is also supported directly
    name: "Myanmar: Disaster Response 3"
            """,
                language="yaml",
            )

    # If configuration is loaded, show editor and processing options
    if config_data:
        st.header("Configuration Review")

        # Configuration editor tabs
        tab1, tab2, tab3 = st.tabs(["HDX Settings", "Dataset Settings", "Projects"])

        with tab1:
            st.subheader("HDX Settings")
            hdx_site = st.selectbox(
                "HDX Site",
                ["demo", "prod"],
                index=0 if config_data.get("hdx_site") == "demo" else 1,
            )

            hdx_api_key = st.text_input(
                "HDX API Key", value=config_data.get("hdx_api_key", ""), type="password"
            )

            hdx_owner_org = st.text_input(
                "HDX Owner Organization", value=config_data.get("hdx_owner_org", "")
            )

            hdx_maintainer = st.text_input(
                "HDX Maintainer", value=config_data.get("hdx_maintainer", "")
            )

            # Update the config with the values from the UI
            config_data["hdx_site"] = hdx_site
            config_data["hdx_api_key"] = hdx_api_key
            config_data["hdx_owner_org"] = hdx_owner_org
            config_data["hdx_maintainer"] = hdx_maintainer

        with tab2:
            st.subheader("Dataset Settings")
            dataset_name = st.text_input(
                "Dataset Name",
                value=config_data.get("dataset_name", "MapSwipe Results"),
            )
            dataset_location = st.text_input(
                "Dataset Location", value=config_data.get("dataset_location", "Global")
            )

            frequency_options = [
                "As Needed",
                "Daily",
                "Weekly",
                "Fortnightly",
                "Monthly",
                "Quarterly",
                "Biannually",
                "Annually",
            ]
            dataset_frequency = st.selectbox(
                "Dataset Update Frequency",
                frequency_options,
                index=frequency_options.index(
                    config_data.get("dataset_frequency", "As Needed")
                )
                if config_data.get("dataset_frequency") in frequency_options
                else 0,
            )

            # Tags input
            default_tags = config_data.get("dataset_tags", ["mapswipe", "crowdsourced"])
            tags_input = st.text_area(
                "Dataset Tags (one per line)", value="\n".join(default_tags)
            )
            dataset_tags = [
                tag.strip() for tag in tags_input.split("\n") if tag.strip()
            ]

            # Format selection
            available_formats = ["geojson", "gpkg", "kml", "shp"]
            selected_formats = st.multiselect(
                "Output Formats",
                available_formats,
                default=config_data.get("file_formats", ["geojson"]),
            )

            # Update the config with the values from the UI
            config_data["dataset_name"] = dataset_name
            config_data["dataset_location"] = dataset_location
            config_data["dataset_frequency"] = dataset_frequency
            config_data["dataset_tags"] = dataset_tags
            config_data["file_formats"] = selected_formats

        with tab3:
            st.subheader("MapSwipe Projects")
            projects = config_data.get("projects", [])

            # Display existing projects
            for i, project in enumerate(projects):
                col1, col2, col3 = st.columns([3, 3, 1])
                with col1:
                    projects[i]["project_id"] = st.text_input(
                        f"Project {i + 1} ID",
                        value=project.get("project_id", ""),
                        key=f"proj_id_{i}",
                    )
                with col2:
                    projects[i]["name"] = st.text_input(
                        f"Project {i + 1} Name",
                        value=project.get("name", f"Project {i + 1}"),
                        key=f"proj_name_{i}",
                    )
                with col3:
                    if st.button("Remove", key=f"remove_{i}"):
                        projects.pop(i)
                        st.rerun()

            # Add new project
            if st.button("Add Project"):
                projects.append(
                    {"project_id": "", "name": f"Project {len(projects) + 1}"}
                )
                config_data["projects"] = projects
                st.rerun()

            # Update the config with the modified projects
            config_data["projects"] = [p for p in projects if p.get("project_id")]

        # Processing section
        st.header("Processing")

        # Validate configuration before processing
        ready_to_process = (
            hdx_api_key
            and hdx_owner_org
            and hdx_maintainer
            and dataset_name
            and dataset_location
            and dataset_frequency
            and dataset_tags
            and selected_formats
            and any(p.get("project_id") for p in projects)
        )

        if ready_to_process:
            if st.button("Process and Publish to HDX", type="primary"):
                with st.spinner("Processing MapSwipe data and publishing to HDX..."):
                    try:
                        # Create a temporary config file
                        with tempfile.NamedTemporaryFile(
                            mode="w", delete=False, suffix=".yaml"
                        ) as tmp:
                            yaml.dump(config_data, tmp)
                            tmp_path = tmp.name

                        # Process the data using our app functions
                        config = Config(tmp_path)

                        # Show progress
                        progress_bar = st.progress(0)
                        status_text = st.empty()

                        # Fetch and aggregate data
                        status_text.text("Fetching MapSwipe project data...")
                        data_aggregator = MapSwipeDataAggregator(config)
                        progress_bar.progress(25)

                        combined_yes_maybe, combined_aoi = (
                            data_aggregator.aggregate_project_data()
                        )
                        progress_bar.progress(50)

                        if not combined_yes_maybe.empty and not combined_aoi.empty:
                            status_text.text("Creating and uploading dataset to HDX...")
                            dataset_creator = HDXDatasetCreator(config)
                            progress_bar.progress(75)

                            dataset_link = dataset_creator.create_and_upload_dataset(
                                combined_yes_maybe, combined_aoi
                            )
                            progress_bar.progress(100)

                            st.success(
                                f"Successfully processed and published data to HDX! {dataset_link}"
                            )
                            st.balloons()
                        else:
                            st.error(
                                "No valid data fetched from the provided MapSwipe projects."
                            )

                        # Clean up temporary file
                        os.unlink(tmp_path)

                    except Exception as e:
                        st.error(f"An error occurred: {str(e)}")
                        raise e
        else:
            st.warning(
                "Please fill in all required configuration values before processing."
            )

    # Footer
    st.markdown("---")
    st.markdown("Created by HOT - Humanitarian OpenStreetMap Team")


if __name__ == "__main__":
    main()
