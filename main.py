import json
from datetime import datetime
from abc import abstractmethod
from time import sleep
from typing import List

import redis as redis
import requests as requests


class APIToJsonFileReader:
    @staticmethod
    def api_to_json_file(url, file_name):
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            with open(file_name, 'w') as f:
                json.dump(data, f)
                print(f'Data saved to {file_name}')
        else:
            raise RuntimeError(f'Error loading data: {response.status_code}')


class JsonReader:
    @staticmethod
    def json_file_to_dict(json_file_path):
        with open(json_file_path, 'rb') as json_file:
            return json.loads(json_file.read())


class ConfigReader:
    @staticmethod
    def read_config(config_file_path):
        config_dict = JsonReader.json_file_to_dict(config_file_path)
        if config_dict['strategy'] == 'console':
            config_dict['strategy'] = PrintToConsoleStrategy
        elif config_dict['strategy'] == 'redis':
            config_dict['strategy'] = SaveInRedisStrategy
        else:
            raise ValueError('Wrong strategy in config')
        return config_dict


class OutputDataInterface:
    @staticmethod
    @abstractmethod
    def output(data):
        pass


class PrintToConsoleStrategy(OutputDataInterface):
    @staticmethod
    def output(data):
        for key in data[0].keys():
            print(key, end=', ')
        print()
        for row in data:
            for value in row.values():
                print(value, end=', ')
            print()
            sleep(0.1)


class SaveInRedisStrategy(OutputDataInterface):
    @classmethod
    def output(cls, data):
        r = redis.Redis(host='localhost', port=6379, db=0)
        print('Connected to Redis')

        for d in data:
            key = f"{datetime.now().timestamp()}:{':'.join(str(v) for v in d.values())}"
            r.hset(key, key, json.dumps(d))

        # Print saved data from Redis
        data_list = []
        for key in r.keys():
            for value in r.hgetall(key).values():
                data_list.append({key.decode("utf-8"): json.loads(value.decode("utf-8"))})

        with open('redis_output.json', 'w') as f:
            json.dump(data_list, f)

        print("Data successfully saved in Redis")


class Context:
    def __init__(self, json_file_path: str, strategy: OutputDataInterface = PrintToConsoleStrategy):
        self.dataset: List[dict] = JsonReader.json_file_to_dict(json_file_path)
        self.strategy = strategy

    def set_dataset(self, data: List[dict]):
        self.dataset = data

    def set_strategy(self, strategy: OutputDataInterface):
        self.strategy = strategy

    def output_data(self):
        self.strategy.output(self.dataset)


if __name__ == '__main__':
    config = ConfigReader.read_config('config.json')
    APIToJsonFileReader.api_to_json_file(config['data_url'], config['json_file_path'])
    context = Context(json_file_path=config['json_file_path'], strategy=config['strategy'])
    context.output_data()
