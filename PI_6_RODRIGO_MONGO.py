import pymongo
import json

def create_collection(client,col_name):
    new_col = client[col_name]
    return new_col

def list_collections(client):
    print(client.list_collection_names())

def add_record(col, new_dict):
    return col.insert_one(new_dict)

def records_in_col(col):
    return col.find()

def make_query(col, query,specify={"_id:":0}):
    return list(col.find(query, specify))

def dump_database(file_path,col):
    with open(file_path) as file:
        for line in file.readlines():
            l = json.loads(line)
            add_record(col, l)

def questionA(col, id):
    reviews = make_query(product_col, { "id": id }, {"_id": 0 ,"reviews": 1})[0]["reviews"]

    reviews.sort(reverse=True,key=lambda x: x["helpful"])

    print("\n##MAIS UTEIS MENOR RATING##")
    for r in reviews[:5]:
        print(r)
    
    reviews.sort(reverse=True,key=lambda x: x["rating"])
    print("\n##MAIS UTEIS MENOR RATING##")
    for r in reviews[-5:]:
        print(r)

def questionB(col, id):
    result = make_query(product_col, { "ASIN": id }, {"_id": 0 ,"similar_items": 1, "salesrank": 1})[0]

    salesrank = int(result["salesrank"])
    similars = result["similar_items"]
    
    for sim in similars:
        #sim_salesrank = int(make_query(product_col, {"ASIN": sim}, {"_id":0, "salesrank":1})
        q = make_query(product_col, {"ASIN":sim}, {"salesrank":1})
        if( len(q) > 0):
            sim_salesrank = int(q[0]["salesrank"])

            if(sim_salesrank < salesrank):
                print(sim)
        

def toDays(date_str):
    return int(date_str[0])*365 + int(date_str[1])*30 + int(date_str[2])
def questionC(col, id):
    result = make_query(product_col, { "ASIN": id }, {"_id": 0 ,"reviews": 1})[0]["reviews"]

    rats = list(map(lambda x: (x['date'],toDays(x['date'].split("-")),x['rating']), result))

    rats.sort(key=lambda x:x[1])

    rats2 = list(map(lambda x: (x[0],x[2]), rats))

    acum = 0
    cont = 0
    for rat in rats2:
        acum+=rat[1]
        cont+=1
        print("DAY:",rat[0], " -> AVG",'{0:.3g}'.format(acum/cont))

def questionD(col, id):
    dict_cat = dict({})
    result = col.find({},{"_id": 0 ,"categories": 1, "salesrank": 1, "ASIN": 1})
    
    for rev in result:
        if(len(rev.keys())==3):
            #print(rev.keys())
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
        
    

        

        
    # r2 = list(map(lambda x:x.split("|")[-1], result))

    # for r in r2:
    #     print(r)
#********************************************************************************************
FILE_PATH = "data/output.txt" 
PRODUCT_ID = "0812550749"


myclient = pymongo.MongoClient("mongodb://localhost:27017/")

mydb = myclient["trab6db"]

product_col = create_collection(mydb, "product")

#************************************************************************************


dump_database(FILE_PATH, product_col)
print("QUESTAO A")
questionA(product_col, PRODUCT_ID)
print("QUESTAO B")
questionB(product_col, PRODUCT_ID)
print("QUESTAO C")
questionC(product_col, PRODUCT_ID)
print("QUESTAO D")
questionD(product_col, PRODUCT_ID)