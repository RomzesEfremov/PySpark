import json
with open("jsonformatter.txt", mode='r') as file:
    __configs = json.load(file)

def get_password() -> str:
    return __configs['password']



def get_name() -> str:
    return __configs['user']


def get_host() -> str:
    return __configs['host']


def db_mane() -> str:
    return __configs['db_name']


def db_url() -> str:
    return __configs['url']

def get_sours_path() -> str:
    return __configs['sours_path']
