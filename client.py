import httplib

# use this server for production, once it's on ec2
SERVER = ''


def get_avg_price():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/digital_music/price/average')
    resp = h.getresponse()
    out = resp.read()
    return out

def get_price_per_brand():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/digital_music/price/average_per_brand')
    resp = h.getresponse()
    out = resp.read()
    return out

def get_grade_distribution():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/digital_music/grade/distribution')
    resp = h.getresponse()
    out = resp.read()
    return out

def get_reviews_count():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/digital_music/reviews/count')
    resp = h.getresponse()
    out = resp.read()
    return out

def get_top_ratings(N):
    out = dict()
    N = str(N)
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/digital_music/rating/top/'+N)
    resp = h.getresponse()
    out = resp.read()
    return out



if __name__ == '__main__':
    print "************************************************"
    print "test of my flask app running at ", SERVER
    print "created by Felipe Formenti Ferreira"
    print "************************************************"
    print " "
    print "******** Average album price **********"
    print get_avg_price()
    print " "
    print "******** Average album price per brand **********"
    print get_price_per_brand()
    print " "
    print "******** Grade Distribution **********"
    print get_grade_distribution()
    print " "
    print "******** Review counts per album **********"
    print get_reviews_count()
    print " "
    print "******** Top N = 10 best rated albums with at leat 21 revies **********"
    print get_top_ratings(10)
    print " "


