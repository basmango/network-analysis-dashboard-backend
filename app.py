from flask import Flask,jsonify,request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import text
import json
import seaborn as sns
from flask_cors import CORS
from datetime import datetime
app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres@localhost:5432/ticketing_dashboard'

db = SQLAlchemy(app)

cors = CORS(app, resources={r"*": {"origins": "*"}})

@app.route("/")
def root():
    return "<h1 style='color:blue'>Hello There!</h1>"

@app.route("/routes")
def routes():
    result = db.session.execute('select route_long_name from routes order by route_long_name;')
    result  = list(dict(i) for i in result.all())
    return jsonify(json_list = result)


@app.route("/dates")
def fetch_dates():
    result = db.session.execute('Select (max(booking_time)) from tickets')
    res = list(result)[0].max
    print(res)
    return jsonify(max_date = res)


@app.route("/stops")
def stops():
    
    route = request.args.get("route")
    
    stops_query = text("Select DISTINCT(stop_name,stop_sequence),stop_name,stop_sequence from route_stop_mapping  where route_long_name  =  :route order by stop_sequence;  ")

    stops  = db.session.execute(stops_query,{'route':route})
    
    stop_names  = [i.stop_name for i in stops]
    
    return jsonify(stops = stop_names)

@app.route("/chartdata")
def chart_data():
    route  =request.args.get("route")
    startingDate = request.args.get('startingDate')
    endingDate = request.args.get('endingDate')
    text1= text("Select user_start_stop_name,SUM(ticket_count) from tickets where route_long_name =  :route  and booking_time between :startdate and :enddate group by user_start_stop_name ;")
    text2= text("Select user_end_stop_name,SUM(ticket_count) from tickets where route_long_name =  :route and booking_time between :startdate and :enddate group by user_end_stop_name;")
    stops_query = text("Select DISTINCT(stop_name,stop_sequence),stop_name,stop_sequence from route_stop_mapping  where route_long_name  =  :route  order by stop_sequence;  ")
    dic_onboarding = {}
    dic_eliding = {}
    
    
    
    onboarding = db.session.execute(text1,{'route':route,'startdate':startingDate,'enddate':endingDate})
    eliding = db.session.execute(text2,{'route' :route,'startdate':startingDate,'enddate':endingDate})
    stops  = db.session.execute(stops_query,{'route':route})
    #stops  = list(dict(i) for i in stops.all())
    for i in onboarding:
        dic_onboarding[i.user_start_stop_name] = i.sum;
        
    for i in eliding:
        dic_eliding[i.user_end_stop_name] = i.sum;
        
    
    stop_arr = []
    onboarding_arr = []
    offloading_arr = []
    for i in stops:
        on_count = el_count=0;
        if i.stop_name in dic_onboarding.keys():
            on_count = dic_onboarding[i.stop_name]
        if i.stop_name in dic_eliding.keys():
            el_count = dic_eliding[i.stop_name]
        
        stop_arr.append(i.stop_name)
        onboarding_arr.append(on_count)
        offloading_arr.append(el_count)   
    
    
    
    return jsonify(stop_arr,onboarding_arr,offloading_arr)


@app.route("/stopbarchart")
def stop_bar_data():
    # need to get data, 
    
    stop   = request.args.get("stop")
    startingDate = request.args.get("startingDate")
    endingDate = request.args.get("endingDate")
    # query database for tuple stats <stop,route,onboarding/eliding/count>
    #prepare onboarding/eliding array and prepare toggle on frontend
    
    text1= text("Select route_long_name,user_start_stop_name,user_end_stop_name,SUM(ticket_count) from tickets where (user_start_stop_name = :stop or user_end_stop_name = :stop) and booking_time between :startingDate and :endingDate  group by user_start_stop_name,user_end_stop_name,route_long_name order by route_long_name; ")
    
    onboarding_stops = []
    eliding_stops = []
    onboarding_data = []
    eliding_data = []
    route_dic_onboarding = {}
    route_dic_eliding = {}
    onboarding_stop_set = set()
    eliding_stop_set = set()
    onboarding_stop_count = {}
    eliding_stop_count = {}
    
    result  = db.session.execute(text1,{'stop':stop,'startingDate':startingDate,'endingDate':endingDate})
    #iterate,populate route dic with tuple of stop,onboarding and eliding.
     
    # collect layer for all s
    for row in result:
        if row.user_start_stop_name == stop:
            eliding_stop_set.add(row.user_end_stop_name)
            if row.user_end_stop_name not in eliding_stop_count.keys():  
                    eliding_stop_count[row.user_end_stop_name] = row.sum
            else:
                    eliding_stop_count[row.user_end_stop_name] += row.sum    
            if row.route_long_name not in route_dic_eliding.keys():
                route_dic_eliding[row.route_long_name] = {row.user_end_stop_name: row.sum}
                    
            else:
                route_dic_eliding[row.route_long_name][row.user_end_stop_name]=row.sum
        else:    
            onboarding_stop_set.add(row.user_start_stop_name)
            if row.user_start_stop_name not in onboarding_stop_count.keys():  
                    onboarding_stop_count[row.user_start_stop_name] = row.sum
            else:
                    onboarding_stop_count[row.user_start_stop_name] += row.sum    
            
            if row.route_long_name not in route_dic_onboarding.keys():
                route_dic_onboarding[row.route_long_name] = {row.user_start_stop_name:row.sum}
            else:
                route_dic_onboarding[row.route_long_name][row.user_start_stop_name]=row.sum
    
    limited_onboarding_labels = sorted(list(onboarding_stop_set),key = lambda x : -onboarding_stop_count[x])[:min(len(onboarding_stop_set),20)]
    limited_eliding_labels = sorted(list(eliding_stop_set),key = lambda x : -eliding_stop_count[x])[:min(len(eliding_stop_set),20)]
    limited_onboarding_labels = [i for i in limited_onboarding_labels if onboarding_stop_count[i] > 0 ]
    limited_eliding_labels = [i for i in limited_eliding_labels if eliding_stop_count[i] > 0 ]
    onboarding_dic = {'labels':limited_onboarding_labels,
                        'datasets':[]
                      }
    eliding_dic ={'labels':limited_eliding_labels,
                        'datasets':[]
                      }
    color_count = len(route_dic_onboarding.keys())
    pallete = sns.color_palette(None,color_count)
    for j,color in zip(route_dic_onboarding.keys(),pallete.as_hex()):
        onboarding_dic['datasets'] += [{'backgroundColor':color,'stack':1,'label':j,'data':[int(route_dic_onboarding[j][i]) if i in route_dic_onboarding[j].keys() else 0 for i in limited_onboarding_labels]}]
    color_count = len(route_dic_eliding.keys())
    pallete = sns.color_palette(None,color_count)
    for j,color in zip(route_dic_eliding.keys(),pallete.as_hex()):
        eliding_dic['datasets'] += [{'backgroundColor':color,'stack':1,'label':j,'data':[int(route_dic_eliding[j][i]) if i in route_dic_eliding[j].keys() else 0 for i in limited_eliding_labels]}]
    
        
    # prepare json arrays as needed by chartjs
    
    
   
    final_dict = {'arrivals':onboarding_dic,'departures':eliding_dic}    
    return jsonify(final_dict)





@app.route("/stop-pie")
def stop_net_pie():
    # need to get data, 
    
    stop   = request.args.get("stop")
    startingDate = request.args.get("startingDate")
    endingDate = request.args.get("endingDate")
    # query database for tuple stats <stop,route,onboarding/eliding/count>
    #prepare onboarding/eliding array and prepare toggle on frontend
    
    text1= text("Select user_start_stop_name,user_end_stop_name,SUM(ticket_count) from tickets where (user_start_stop_name = :stop or user_end_stop_name = :stop) and booking_time between :startingDate and :endingDate group by user_start_stop_name,user_end_stop_name  ")
    
    onboarding_count = eliding_count = 0;
    
    result  = db.session.execute(text1,{'stop':stop,'startingDate':startingDate,'endingDate':endingDate})
    
    
    for row in result:
        if row.user_start_stop_name == stop: 
            onboarding_count+=row.sum    
        else:
            eliding_count+=row.sum
            
    data = {
        'labels': ["Departures","Arrivals"],
         'datasets':[{
             'label':'Onboarding-Eliding Chart',
             'data': [onboarding_count,eliding_count],
             'backgroundColor': [ 'rgba(255, 22, 22,1)',
                 'rgba(22, 22, 100, 1)']
         }]
         ,
            
    }
    
    return jsonify(data)



@app.route("/stop-onboarding-donut")
def stop_onboarding_donut():
    # need to get data, 
    
    stop   = request.args.get("stop")
    startingDate = request.args.get("startingDate")
    endingDate = request.args.get("endingDate")
    # query database for tuple stats <stop,route,onboarding/eliding/count>
    #prepare onboarding/eliding array and prepare toggle on frontend
    
    dic_route_count = {}
    
    text1= text("Select route_long_name,SUM(ticket_count) as count from tickets where (user_start_stop_name = :stop ) and booking_time between :startingDate and :endingDate  group by route_long_name order by count ;  ")
    
    
    
    result  = db.session.execute(text1,{'stop':stop,'startingDate':startingDate,'endingDate':endingDate})
    
    dic_onboarding_route_count = {}
    labels_arr = []
    counts_arr =  []
    
    for row in result:
        labels_arr += [row.route_long_name]
        counts_arr += [row.count]
    
    pallete = sns.color_palette(None,len(labels_arr))
    
    data = {
        'labels': labels_arr,
         'datasets':[{
             'label':'Onboarding-Routes Chart',
             'data': counts_arr,
             'backgroundColor': list(pallete.as_hex())
         }]
         ,
            
    }
    
    return jsonify(data)

@app.route("/stop-hourly-distribution")
def stop_hour_line():
    stop  = request.args.get("stop")
    startingDate = request.args.get("startingDate")
    endingDate = request.args.get("endingDate")
    txt1 =text("select date_part('hour',booking_time),SUM(ticket_count) as count from tickets where user_end_stop_name = :stop  and booking_time between :startingDate and :endingDate  group BY date_part('hour',booking_time);")
    result1  = db.session.execute(txt1,{'stop':stop,'startingDate':startingDate,'endingDate':endingDate})
    txt2 =text("select date_part('hour',booking_time),SUM(ticket_count) as count from tickets where user_start_stop_name = :stop  and booking_time between :startingDate and :endingDate  group BY date_part('hour',booking_time);")
    result2  = db.session.execute(txt2,{'stop':stop,'startingDate':startingDate,'endingDate':endingDate})
    labels =  [f"{i}:00" for i in range(0,25)]
    


    dict_arrivals = {}
    dict_departures = {}
    
    for row in result1:
        dict_arrivals[f"{round(row.date_part)}:00"] = row.count

    for row in result2:
        dict_departures[f"{round(row.date_part)}:00"] = row.count
        
    arrivals = [dict_arrivals[i] if i in dict_arrivals.keys() else 0 for i in labels ]
    
    departures = [dict_departures[i] if i in dict_departures.keys() else 0 for i in labels ]


    return jsonify(labels = labels,arrivals=arrivals,departures = departures)

@app.route("/stop-eliding-donut")
def stop_eliding_donut():
    # need to get data, 
    
    stop  = request.args.get("stop")
    startingDate = request.args.get("startingDate")
    endingDate = request.args.get("endingDate")
    # query database for tuple stats <stop,route,onboarding/eliding/count>
    #prepare onboarding/eliding array and prepare toggle on frontend
    
    dic_route_count = {}
    
    text1= text("Select route_long_name,SUM(ticket_count) as count from tickets where (user_end_stop_name = :stop ) and booking_time between :startingDate and :endingDate  group by route_long_name order by count ;  ")
    
    
    
    result  = db.session.execute(text1,{'stop':stop,'startingDate':startingDate,'endingDate':endingDate})
    
    dic_eliding_route_count = {}
    labels_arr = []
    counts_arr =  []
    
    for row in result:
        labels_arr += [row.route_long_name]
        counts_arr += [row.count]
    
    pallete = sns.color_palette(None,len(labels_arr))
    
    data = {
        'labels': labels_arr,
         'datasets':[{
             'label':'Eliding-Routes Chart',
             'data': counts_arr,
             'backgroundColor': list(pallete.as_hex())
         }]
         ,
            
    }
    
    return jsonify(data)


    