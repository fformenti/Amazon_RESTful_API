import json
import ast
import psycopg2, psycopg2.extras

# DSN location of the AWS - RDS instance
DB_DSN = ""

# location of the input data file
REVIEW_DATA = 'sub_reviews.json'
META_DATA = 'sub_meta.json'


def aux_fill(json_obj, field):
    """
    check if the field exists. If not it create swith a null value
    """
  try:
    aux = json_obj[field]
  except:
    aux = None
  return aux


def transform_meta_data(filename):

  """
  transforms a file with json into tuples
  returns: tuples
  """
  data = list()
  try:
    f = open(filename)
    for line in f:
      json_obj = json.loads(line)
      asin = aux_fill(json_obj, 'asin')
      title = aux_fill(json_obj, 'title')
      description = aux_fill(json_obj,'description')
      price = aux_fill(json_obj,'price')
      related = aux_fill_json(json_obj,'related')
      salesRank = aux_fill_json(json_obj,'salesRank')
      brand = aux_fill(json_obj,'brand')
      my_tuple = (asin, title, description, price, json.dumps(related), json.dumps(salesRank), brand)
      data.append(my_tuple)
    f.close()
  except Exception as e:
      print e

  return data

def transform_review_data(filename):
  """
  transforms a file with json into tuples
  returns: tuples
  """
    data = list()
    try:
        f = open(filename)
        for line in f:
          try:
            json_obj = json.loads(line)
            reviewerID = json_obj["reviewerID"]
            asin = json_obj["asin"]
            reviewerName = json_obj["reviewerName"]
            helpful = json_obj["helpful"]
            reviewText = json_obj["reviewText"]
            overall = json_obj["overall"]
            summary = json_obj["summary"]
            reviewTime = json_obj["reviewTime"]
            my_tuple = (reviewerID, asin, reviewerName, helpful, reviewText, overall, summary, reviewTime)
            data.append(my_tuple)
          except:
            pass
        f.close()
    except Exception as e:
        print e

    return data

def create_table(query):

    try:
       conn = psycopg2.connect(dsn=DB_DSN)
       cur = conn.cursor()
       cur.execute(query)
       conn.commit()
    except psycopg2.Error as e:
       print e.message
    else:
       cur.close()
       conn.close()

def insert_review_data(data):

    try:
       sql = "INSERT INTO reviews VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
       conn = psycopg2.connect(dsn=DB_DSN)
       cur = conn.cursor()
       cur.executemany(sql, data) # NOTE executemany() as opposed to execute()
       conn.commit()
    except psycopg2.Error as e:
       print e.message
    else:
       cur.close()
       conn.close()

def insert_meta_data(data):

    try:
       sql = "INSERT INTO meta VALUES(%s, %s, %s, %s, %s, %s, %s)"
       conn = psycopg2.connect(dsn=DB_DSN)
       cur = conn.cursor()
       cur.executemany(sql, data) # NOTE executemany() as opposed to execute()
       conn.commit()
    except psycopg2.Error as e:
       print e.message
    else:
       cur.close()
       conn.close()

def drop_table(my_table):
    
    try:
       sql = "DROP TABLE IF EXISTS " + my_table + ";"
       conn = psycopg2.connect(dsn=DB_DSN)
       cur = conn.cursor()
       cur.execute(sql)
       conn.commit()
    except psycopg2.Error as e:
       print e.message
    else:
       cur.close()
       conn.close()

if __name__ == '__main__':

  print "******* dropping review table **********"
  drop_table('reviews')

  print "******* creating table review **********"
  sql = "create table reviews (reviewerID TEXT, asin TEXT, reviewerName TEXT, helpful INTEGER[], reviewText TEXT, overall INT, summary TEXT, reviewTime TEXT);"
  create_table(sql)

  print "******* transforming review data **********"
  review_data = transform_review_data(REVIEW_DATA)

  print "******* inserting data into review table **********"
  insert_review_data(review_data)

  print "******* dropping meta table **********"
  drop_table('meta')

  print "******* creating meta review **********"
  sql = "create table meta (asin TEXT, title TEXT, description TEXT, price REAL, related JSON, salesRank JSON, brand TEXT);"
  create_table(sql)

  print "******* transforming meta data **********"
  meta_data = transform_meta_data(META_DATA)

  print "******* inserting data into meta table **********"
  insert_meta_data(meta_data)

