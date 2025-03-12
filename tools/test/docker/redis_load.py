import redis
import json
import logging

def main():
    file_list = [("testdata/json_tests/appl_state_db.txt", 14),
                 ("testdata/json_tests/asic_db.txt", 1),
                 ("testdata/json_tests/counters_db.txt", 2),
                 ("testdata/json_tests/config_db.txt", 4),
                 ("testdata/json_tests/state_db.txt", 6)]
    for f, db in file_list:
        r = redis.StrictRedis(host='127.0.0.1', port=6379, db=db)  # update your redis settings
        cache_timeout = None
        try:
            with open(f) as f:
                data = json.load(f)
                for key in data:
                    for dkey in data.get(key).keys():
                        r.hset(key, str(dkey), str(data.get(key)[dkey]))
                print('Data loaded into redis successfully')
        except Exception as e:
            print(e)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    main()
