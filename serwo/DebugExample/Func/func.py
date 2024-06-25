import json
from python.src.utils.classes.commons.serwo_objects import SerWOObject
import os, uuid


def user_function(serwoObject) -> SerWOObject:
    try:
        fin_dict = dict()
        data = serwoObject.get_body()
        print("Data to push - ", data)
        metadata = serwoObject.get_metadata()
        fin_dict["data"] = "success: OK"
        fin_dict["metadata"] = metadata
        print("Fin dict - ", fin_dict)
        data = {"body": "success: OK"}
        return SerWOObject(body=data)
    except Exception as e:
        return SerWOObject(error=True)
