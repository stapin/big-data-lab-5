import os
import yaml


class SparkConfigManager:
    def __init__(self, config_file="spark_config.yaml"):
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Configuration file {config_file} not found!")

        with open(config_file, 'r', encoding='utf-8') as f:
            yaml_content = f.read()

        self._config = yaml.safe_load(yaml_content)

    def get_spark_config(self, profile_name: str) -> dict:
        profiles = self._config.get("spark_profiles", {})
        if profile_name not in profiles:
            raise ValueError(f"Spark profile '{profile_name}' not found in config.yaml")

        return profiles[profile_name]

spark_conf_manager = SparkConfigManager()
