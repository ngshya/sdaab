import yaml
import os
from pathlib import Path
from os.path import dirname, realpath
from .logger import logger


def get_config(path_env_yaml=None):
    '''
    Get the configuration for the package.
    
    Parameters
    ----------
    path_env_yaml : str, optional
        Location of the YAML configuration file, by default None
    
    Returns
    -------
    dict
        A dictionary containing configuration parameters.
    '''

    if path_env_yaml is None:
        if "ENV_RUN" not in os.environ:
            logger.info("Environment not defined in os.environ, set DEVELOPMENT as default value.")
            os.environ["ENV_RUN"] = "DEVELOPMENT"
        str_env = os.environ["ENV_RUN"].lower()
        logger.info("Environment set to " + str_env + ".")
        path_env_yaml = Path(dirname(dirname(dirname(realpath(__file__)))) + "/config/" + str_env + ".yml")

    if os.path.isfile(path_env_yaml):
        dict_config = yaml.safe_load(open(path_env_yaml, "r"))
    else:
        logger.warning("YAML file not found, loading default configuration.")
        dict_config = {
            "ENV": "DEFAULT", 
            "PING": "PONG", 
            "PATH_IN": "IN",
            "PATH_OUT": "OUT", 
            "PATH_SAVE": "SAVE"
        }

    return dict_config


dict_config = get_config()