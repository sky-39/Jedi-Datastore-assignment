import memcache

servers = ['127.0.0.1:11211', '127.0.0.1:11212', '127.0.0.1:11213']
mc = memcache.Client(servers)

def get_data(key):
    return mc.get(key)

def put_data(key, value):
    mc.set(key, value)

def delete_data(key):
    mc.delete(key)


if __name__ == "__main__":
    key = "test_key"
    value = "test_value"

    put_data(key,value)
    print(f"Inserted value: {value} with key: {key} into memcached.")

    retrieved_value = get_data(key)

    if(retrieved_value):
        print(f"Retrieved value for key: {key} is {retrieved_value}")
    else:
        print(f"No value found for key: {key}")

    