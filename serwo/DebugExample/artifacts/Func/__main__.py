import os
import time
import psutil
import objsize
from copy import deepcopy

from func import user_function as func_function

from python.src.utils.classes.commons.serwo_objects import SerWOObject
from python.src.utils.classes.commons.serwo_objects import build_serwo_object
from python.src.utils.classes.commons.serwo_objects import build_serwo_list_object


def main(event):
    print("*" * 10)
    print("Function ran")
    print(event)
    print("*" * 10)

    return {
        "status": "success"
    }

if __name__ == "__main__":
    print("Main Method: Nothing is executed")