from flask import Flask, request, jsonify
import psycopg2, psycopg2.extras

# DSN location of the AWS - RDS instance
DB_DSN = "host= xxxx.rds.amazonaws.com dbname=dbproject user=felipeformenti"

app = Flask(__name__)

@app.route('/')
def home():
	return "No_SQL Project by Felipe Formenti Ferreira"

@app.route('/digital_music/price/average')
def get_avg_price():
    """
    calculates the average price of the albums
    :return: a dict of key = avg and value = average
    """
    out = dict()

    sql = "select avg(price) as avg from meta ORDER BY avg DESC;"

    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        print rs
        for item in rs:
            out['avg'] = item['avg']

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

@app.route('/digital_music/price/average_per_brand')
def get_price_per_brand():

    """
    calculates the average price of the albums per brand
    :return: a dict of key = brand and value = average
    """

    out = dict()

    sql = "select brand, avg(price) as avg from meta \
    where brand NOTNULL GROUP BY brand ORDER BY avg DESC;"

    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        print rs
        for item in rs:
            avg = item['avg']
            out[item['brand']] = avg

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)


@app.route('/digital_music/grade/distribution')
def get_grade_distribution():

    """
    calculates the rating distribution
    :return: a dict of key = grade and value = count
    """

    out = dict()

    sql = "select overall, count(*) as my_count from reviews GROUP BY overall ORDER BY overall;"
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        print rs
        for item in rs:
            my_count = item['my_count']
            out[item['overall']] = my_count

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)


@app.route('/digital_music/reviews/count')
def get_reviews_count():

    """
    calculates number of reviews per album
    :return: a dict of key = album and value = count
    """

    out = dict()

    sql = "select LHS.title as title, top.N_count as Total_Reviews from \
    (select asin, title from meta) as LHS  inner join \
    (select asin, count(asin) as N_count from reviews \
    GROUP BY asin) as top On LHS.asin = top.asin ORDER BY Total_Reviews DESC;"

    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        print rs
        for item in rs:
            total_reviews = item['total_reviews']
            out[item['title']] = total_reviews

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)


@app.route('/digital_music/rating/top/<N>')
def get_top_ratings(N):

    """
    calculates the top rated albums with at least 21 reviews
    :return: a dict of key = brand and value = average
    """

    out = dict()

    sql = "select LHS.title as title, top.avg_rate as rating from(select asin, title from meta) as LHS \
    inner join \
    (select asin, avg(overall) as avg_rate , count(*) as count_reviews from reviews \
    GROUP BY asin) as top \
    On LHS.asin = top.asin \
    where count_reviews > 20 \
    ORDER BY rating DESC \
    LIMIT %s; "

    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, (N, ))
        rs = cur.fetchall()
        for item in rs:
          nota = round(item['rating'],2)
          out[item['title']] = nota
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

if __name__ == '__main__':
	app.debug = True
	app.run(host='0.0.0.0', debug = True, port=5000)



