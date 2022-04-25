# SPDX-FileCopyrightText: 2017-2022 Contributors to the OpenSTEF project <korte.termijn.prognoses@alliander.com>
#
# SPDX-License-Identifier: MPL-2.0


def add_application_metadata(logger, method_name, event_dict):
    """Add additional API metadata to the passed event dictionary.
    Note:
        The unused arguments are still passed by the caller and need to be caught.
    Args:
        logger: wrapped logger object
        method_name: name of the wrapper method
        event_dict: current context and event
    Returns:
        Dictionary with possibly extra fields
    """
    #    if "app_version" not in event_dict:
    #        event_dict["app_version"] = get_setting("APP_VERSION")

    return event_dict


def rename_forbidden_keys(logger, method_name, event_dict):
    """Certain keys should not be used for logging, as they are already
    claimed by kibana. Not respecting the Kibana-claimed-keys results in
    ommitted logging at cluster level.
    This function adds a prefix 'log.' to the forbidden keys


    Example:
        event_dict = dict(event='textmessage', specific.key='toedeloe') ->
        event_dict = dict(log.event='textmessage', specific.key='toedeloe')
    """
    forbidden_keys = ["event"]

    # For all keys in forbidden keys AND event_dict, add prefix 'log.'
    for key in set(forbidden_keys).intersection(set(event_dict.keys())):
        event_dict[f"log.{key}"] = event_dict.pop(key)

    return event_dict
