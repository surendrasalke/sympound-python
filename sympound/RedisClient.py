import redis
import json
# redis_host = "localhost"
# redis_port = 6379
# redis_password = ""
# r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)


class RedisClient():
    def __init__(self, redis_host, redis_port, redis_password):
        self.r = redis.StrictRedis(host=redis_host, port=redis_port,
                                   password=redis_password, decode_responses=True)

    def delete(self, speller_id):
        """delete keys"""
        for key in self.r.scan_iter(speller_id + ":*"):
            self.r.delete(key)


    def get_max_length(self, speller_id):
        """Get value for given key in dictionary"""
        redis_key = speller_id + ': max_length'
        val = self.r.get(redis_key)
        if not val:
            return 0
        else:
            return json.loads(self.r.get(redis_key))

    def set_max_length(self, speller_id, value):
        """Put value for given key in dictionary"""
        redis_key = speller_id + ': max_length'
        return self.r.set(redis_key, json.dumps(value))



    def get_words(self, speller_id, key):
        """Get value for given key in dictionary"""
        redis_key = speller_id + ': words :' + key

        return json.loads(self.r.get(redis_key))

    def set_words(self, speller_id, key, value):
        """Put value for given key in dictionary"""
        redis_key = speller_id + ': words :' + key
        return self.r.set(redis_key, json.dumps(value))

    def exists_words(self,speller_id, key):
        """Check if key is present in deletes dictionary"""
        redis_key = speller_id + ': words :' + key
        if self.r.exists(redis_key):
            return True
        return False



    def mass_put(self, dictionary_name, speller_id, key, dictionary):
        """Bulk upload data to redis"""
        pass




    def get_deletes(self, speller_id, key):
        """Get value for given key in dictionary"""
        redis_key = speller_id + ': deletes :' + key

        return_val =  self.r.get(redis_key)
        if return_val:
            return json.loads(return_val)
        else:
            return list()

    def set_deletes(self, speller_id, key, value):
        """Put value for given key in dictionary"""
        redis_key = speller_id + ': deletes :' + key
        return self.r.set(redis_key, json.dumps([value]))

    def exists_deletes(self,speller_id, key):
        """Check if key is present in deletes dictionary"""
        redis_key = speller_id + ': deletes :' + key
        if self.r.exists(redis_key):
            return True
        return False

    def append_deletes(self, speller_id, key, value):
        """append to deletes dictionary key """
        redis_key = speller_id + ': deletes :' + key
        if self.exists_deletes(speller_id,key):
            val_list = self.get_deletes(speller_id, key)
            val_list.append(value)
            return self.r.set(redis_key, json.dumps(val_list))
        else:
            return self.r.set(redis_key, json.dumps([value]))

    def try_redis(self):

        self.r.set("msg:hello", "Hello Redis!!!")
        msg = self.r.get("msg:hello")
        print(msg)
if __name__ == "__main__":
    r = RedisClient("localhost", 6379, "")
    # redis_gateway.try_redis()
    speller_id = "kt-123"
    dicionary_name = "delete"
    key = "'004b9e608e7063e9e40f58d5e0d365f7' (140294993870056)"
    print(r.set_words(speller_id, key, "Surendra"))
    print(r.get_words(speller_id, key))
    print(r.exists_words(speller_id, key))
    print("----------------")
    print(r.set_deletes(speller_id, key, "Surendra"))
    print(r.get_deletes(speller_id, key))
    print(r.exists_deletes(speller_id, key))
    print(r.append_deletes(speller_id, key, "salke"))
    print(r.get_deletes(speller_id, key))
    max_len = r.get_max_length(speller_id, key)
    print(max_len)