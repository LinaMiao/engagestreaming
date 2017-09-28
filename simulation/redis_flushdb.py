import redis

r1 = redis.StrictRedis(host='54.245.160.86',port=6379,password='127001',db = 1)
r2 = redis.StrictRedis(host='54.245.160.86',port=6379,password='127001',db = 2)
r3 = redis.StrictRedis(host='54.245.160.86',port=6379,password='127001',db = 3)
r4 = redis.StrictRedis(host='54.245.160.86',port=6379,password='127001',db = 4)
r5 = redis.StrictRedis(host='54.245.160.86',port=6379,password='127001',db = 5)
r6 = redis.StrictRedis(host='54.245.160.86',port=6379,password='127001',db = 6)
r7 = redis.StrictRedis(host='54.245.160.86',port=6379,password='127001',db = 7)


for r in [r1, r2, r3, r4, r5, r6, r7]:
    r.flushdb()
