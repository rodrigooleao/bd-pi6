import redis
import json

def make_query(redis, key):
    return redis.get(key).decode("utf-8")


def dump_database(file_path,redis):
    with open(file_path) as file:
        for line in file.readlines():
            l = json.loads(line)
            asin = l["ASIN"]
            del l["ASIN"]

            redis.set(asin, str(l))

def questionA(redis, id):
    reviews = make_query(redis, id)
    
    dict_reviews = json.loads(reviews.replace("\'","\""))
    print(type(dict_reviews))
    revs = dict_reviews["reviews"]
    
    revs.sort(reverse=True,key=lambda x: x["helpful"])

    print("\n##MAIS UTEIS MENOR RATING##")
    for r in revs[:5]:
        print(r)
    
    revs.sort(reverse=True,key=lambda x: x["rating"])
    print("\n##MAIS UTEIS MENOR RATING##")
    for r in revs[-5:]:
        print(r)

def questionB(redis, id):
    result = make_query(redis, id)

    dict_result = json.loads(result.replace("\'","\""))

    salesrank = int(dict_result["salesrank"])
    similars = dict_result["similar_items"]

    for sim in similars:
        #sim_salesrank = int(make_query(product_col, {"ASIN": sim}, {"_id":0, "salesrank":1})
        q = make_query(redis,sim)
        print(q)
        dq = json.loads(q.replace("\'","\""))
        print(dq)
        if( "salesrank" in dq):
            sim_salesrank = int(dq["salesrank"])

            if(sim_salesrank < salesrank):
                print(sim)
def toDays(date_str):
    return int(date_str[0])*365 + int(date_str[1])*30 + int(date_str[2])

def questionC(redis,id):
    result = make_query(redis,id)

    dict_result = json.loads(result.replace("\'","\""))

    reviews = dict_result["reviews"]

    rats = list(map(lambda x: (x['date'],toDays(x['date'].split("-")),x['rating']), reviews))

    rats.sort(key=lambda x:x[1])

    rats2 = list(map(lambda x: (x[0],x[2]), rats))

    acum = 0
    cont = 0
    for rat in rats2:
        acum+=rat[1]
        cont+=1
        print("DAY:",rat[0], " -> AVG",'{0:.3g}'.format(acum/cont))

def questionD(redis, id):
    all_values = []
    elem_dict = dict({})
    for key in redis.keys("*"):
        k = key.decode("utf-8")
        res = make_query(redis,k )
        print(res)
        d_res = str(d_res)
        d_res = json.loads(res.replace("\'","\""))
        if("categories" in d_res and "salesrank" in d_res):
            elem_dict["ASIN"] = key
            elem_dict["categories"] = d_res["categories"]
            elem_dict["salesrank"] = d_res["salesrank"]
            all_values.append(elem_dict)
    
    for rev in all_values:
        asin = rev["ASIN"]
        sr = int(rev["salesrank"])
        
        cats = rev["categories"]
        for cat in cats:
            c = cat.split("|")[-1]
            if(c not in dict_cat):
                dict_cat[c] = [asin]
            else:
                dict_cat[c].append(asin)

    for key in dict_cat:
        print(key, " -> ", dict_cat[key][:10])

r = redis.Redis()

FILE_PATH = "data/output.txt" 
PRODUCT_ID = "0812550749"

dump_database(FILE_PATH,r)
print("QUESTAO A")
questionA(r, PRODUCT_ID)
print("QUESTAO B")
questionB(r, PRODUCT_ID)
print("QUESTAO C")
questionC(r, PRODUCT_ID)
print("QUESTAO D")
questionD(r, PRODUCT_ID)