LOGGING = False
LOG_INTERVAL = 10
LOCAL = False

MIN_SIZE = 128
RE_INSERT_SAMPLES = 8
AUTO_RESPAWN = True

RESPAWN='REINSERT'
#RESPAWN='RANDOM'

HOST="redis-10326.c8.us-east-1-3.ec2.cloud.redislabs.com"
# "pub-redis-13994.us-east-1-3.3.ec2.garantiadata.com"#"pub-redis-17694.us-east-1-3.4.ec2.garantiadata.com"
PORT = 10326#13994#17694
PASS = "evo6"#"evopool6"

# HOST= "localhost"
# PORT= 6379

import os, redis, random

##REDISCLOUD
import urlparse

if os.environ.get('REDISTOGO_URL'):
    url = urlparse.urlparse(os.environ.get('REDISTOGO_URL'))
    r = redis.Redis(host=url.hostname, port=url.port, password=url.password)
#LOCAL
else:
    r = redis.Redis(host=HOST, port=PORT, password=PASS)

class Individual:
    def __init__(self, **kwargs):
        self.id = kwargs['id']
        self.fitness = kwargs.get('fitness',{})
        self.chromosome = kwargs.get('chromosome',[])
        self.specie = kwargs.get('specie')
        self.params = kwargs.get('params',[])
        self.__dict__.update(kwargs)


    def put(self, population):
        pipe = r.pipeline()
        #if pipe.hset(self.specie, self.id, self.__dict__):
        #if pipe.sadd( population, self.id ):
        #    pipe.set( self.id , self.__dict__ )
        #if pipe.hexists(self.specie, self.id):
        #pipe.hdel(self.specie, self.id)
        pipe.hset(self.specie, self.id, self.__dict__)
        pipe.execute()
        return True
        #else:
        #return False

    def get_specie(self):
        return int(self.specie)

    def get(self, specie, as_dict = False):
        if r.hget(specie, self.id):
            _dict = eval(r.hget(specie, self.id))
            self.__dict__.update(_dict)
        else:
            raise LookupError("Key Not Found")

        if as_dict:
            return self.__dict__
        else:
            return self

    def __repr__(self):
        return self.id + ":" + str(self.fitness) + ":" + str(self.chromosome)

    def as_dict(self):
        return self.__dict__


class Specie:
    def __init__(self, **kwargs):
        self.id = kwargs['id']
        self.intra_distance = kwargs.get('intra_distance')
        self.flag_speciation = kwargs.get('flag_speciation')
        self.specie = kwargs.get('specie')
        self.__dict__.update(kwargs)

    def put(self, id):
        pipe = r.pipeline()
        pipe.hset(self.id, id, self.__dict__)
        pipe.execute()
        return True

    def get(self,  as_dict=False):
        if r.hget(self.id, self.id):
            _dict = eval(r.hget(self.id, self.id))
            self.__dict__.update(_dict)
        else:
            raise LookupError("Key Not Found")

        if as_dict:
            return self.__dict__
        else:
            return self

    def as_dict(self):
        return self.__dict__


class Population:
    def __init__(self, name="pop"):
        self.name = name
        self.sample_counter = self.name+':sample_count'
        self.individual_counter = self.name+':individual_count'
        self.specie_counter = self.name + ':specie_count'
        self.sample_queue = self.name+":sample_queue"
        self.returned_counter = self.name+":returned_count"
        self.log_queue = self.name+":log_queue"

    def get_returned_counter(self):
        return int(r.hget('at', self.returned_counter))

    def individual_next_key(self):
        key = r.hincrby('at', self.individual_counter)
        return self.name+":individual:%s" % key

    def size(self):
        return r.hkeys(self.name)

    def initialize(self):
        pattern = "%s*" % self.name
        keys = r.keys(pattern)
        if keys:
            r.delete(*keys)
        #r.flushall()
        r.hsetnx('at', self.sample_counter, 0)
        r.hsetnx('at', self.individual_counter, 0)
        r.hsetnx('at', self.specie_counter, 0)
        r.hsetnx('at', self.returned_counter, 0)
        r.hset('at', self.name + ":found", 0)

    def get_population(self):
        sample_id = r.hincrby('at', self.sample_counter)
        count = int(r.hget('at', self.returned_counter))

        keys = sorted(r.keys())
        keys.pop()
        sample3 = []
        for i in keys:
            sample = r.hgetall(i)
            for key in sample:
                sample3.append(Individual(id=key).get(i, as_dict=True))

        r.sadd(self.name + ":sample:%s" % sample_id, *sample3)
        r.rpush(self.sample_queue, self.name + ":sample:%s" % sample_id)
        try:
            if count % 3 == 0:
                for i in sample3:
                    y = {"specie": None}
                    i.update(y)
                result = {'sample_id': self.name + ":sample:%s" % sample_id,
                          'sample': sample3}
            #     # return result
            else:
                result = {'sample_id': self.name + ":sample:%s" % sample_id,
                          'sample': sample3}
        except:
            return None
        return result

    def get_species(self):
        l_k = []
        keys = sorted(r.keys())
        #keys.pop()
        for e in keys:
            try:
                l_k.append(int(e))
            except:
                None
        return l_k

    def get_sample(self, size):

        if AUTO_RESPAWN and r.scard(self.name) <= MIN_SIZE:
            self.respawn(RE_INSERT_SAMPLES)

        sample_id = r.incr(self.sample_counter)
        # Get keys
        sample = [r.spop(self.name) for i in range(size)]
        # If there is a None
        if None in sample:
            sample = [s for s in sample if s]
            if not sample:
                return None
        r.sadd(self.name+":sample:%s" % sample_id, *sample)
        r.rpush(self.sample_queue, self.name+":sample:%s" % sample_id)
        try:
            result ={'sample_id': self.name+":sample:%s" % sample_id,
                       'sample':   [Individual(id=key).get(as_dict=True) for key in sample ]}
        except:
            return None
        return result

    def get_sample_specie(self, specie):
        if AUTO_RESPAWN and r.scard(self.name) <= MIN_SIZE:
            self.respawn(RE_INSERT_SAMPLES)

        sample_id = r.hincrby('at', self.sample_counter)
        sample2 = []
        sample = r.hgetall(specie)
        for key in sample:
            sample2.append(r.hget(specie, key))
        # If there is a None
        if None in sample:
            sample = [s for s in sample if s]
            if not sample:
                return None
        # r.sadd(self.name + ":sample:%s" % sample_id, *sample)
        r.rpush(self.sample_queue, self.name + ":sample:%s" % sample_id)
        try:
            result = {'sample_id': self.name + ":sample:%s" % sample_id,
                      'sample': [Individual(id=key).get(specie, as_dict=True) for key in sample]}
        except:
            return None
        return result

    def read_sample_queue(self):
        result = r.lrange(self.sample_queue,0,-1)
        return result

    def read_sample_queue_len(self):
        return r.llen(self.sample_queue)

    def read_pop_keys(self):
        sample = r.smembers(self.name)
        sample = list(sample)
        result = {'sample': sample}
        return result

    def read_all(self):
        sample = r.smembers(self.name)
        result = {'sample':   [Individual(id=key).get(as_dict=True) for key in sample]}
        return result

    def get_specieinfo(self, specie):
        id_Specie = "specie:%s" % specie
        sample = Specie(id=id_Specie).get(as_dict=True)
        return sample

    def get_speciedistance(self, specie):
        id_Specie = "specie:%s" % specie
        sample = Specie(id=id_Specie).get(as_dict=True)
        return sample["intra_distance"]

    def get_at_specie(self):
        return r.hget('at', self.specie_counter)

    def put_specieinfo(self, specie):
        if specie['id'] is None:
            specie['id'] = "specie:%s" % specie['specie'] # % r.hincrby('at', self.specie_counter)
        if not r.hexists(specie['id'], specie['id']):
            r.hincrby('at', self.specie_counter)
        specie = Specie(**specie)
        specie.put(specie.id)

    def put_individual(self, **kwargs):
        if kwargs['id'] is None:
            kwargs['id'] = self.name+":individual:%s" % r.hincrby('at',self.individual_counter)
        ind = Individual(**kwargs)
        ind.put(self.name)

    def put_sample(self, sample):
        if not isinstance(sample, dict):
            raise TypeError("Samples must be dictionaries")

        r.hincrby('at', self.returned_counter)

        if LOGGING:
            count = r.hincrby('at',self.returned_counter)
            if count % LOG_INTERVAL == 0 :
                r.sunionstore("log:"+str(count),"pop")

        for member in sample['sample']:
            if member['id'] is None:
                member['id'] = self.name+":individual:%s" % r.hincrby('at',self.individual_counter)
            self.put_individual(**member)
        r.delete(sample['sample_id'])
        r.lrem(self.sample_queue, sample['sample_id'])

    def put_sample_specie(self, sample):
        if not isinstance(sample, dict):
            raise TypeError("Samples must be dictionaries")

        r.hincrby('at', self.returned_counter)

        if LOGGING:
            count = r.hincrby('at', self.returned_counter)
            if count % LOG_INTERVAL == 0:
                r.sunionstore("log:"+str(count), "pop")

        # delete all the individuals of the specie\
        if sample['sample_specie'] is not None:
            if r.exists(sample['sample_specie']):
                r.delete(sample['sample_specie'])

        for member in sample['sample']:
            if member['id'] is None:
                member['id'] = self.name+":individual:%s" % r.hincrby('at', self.individual_counter)
            self.put_individual(**member)
        r.delete(sample['sample_id'])
        r.lrem(self.sample_queue, sample['sample_id'])

    def respawn_sample(self, sample_id):
        if r.exists(sample_id):
            members = r.smembers(sample_id)
            r.sadd(self.name, *members)
            r.delete(sample_id)
            r.lrem(self.sample_queue, sample_id, 1)

    def respawn_ratio(self, ratio=.2):
        until_sample = int(r.llen(self.sample_queue)*ratio)
        for i in range(until_sample):
            self.respawn_sample(r.lpop(self.sample_queue))

    def respawn(self, n=1):
        if RESPAWN == 'REINSERT':
            current_size = r.llen(self.sample_queue)
            if n > current_size:
                for i in range(current_size):
                    self.respawn_sample( r.lpop(self.sample_queue))
            else:
                for i in range(n):
                    self.respawn_sample( r.lpop(self.sample_queue))

    def found(self):
        return r.get(self.name+":found")

    def found_it(self):
        r.set(self.name+":found", 1)